import json
from collections import defaultdict

# ===================== Core helpers =====================

TWO_PORT_TYPES = ["PIPE", "INLCHK", "INLGEN"]

def get_uid_maps(srv, model="PROD"):
    """
    Return two dicts:
      uid_type: {uid: TYPE}
      uid_label: {uid: Label}
    """
    n = int(srv.get_value(f"GAP.MOD[{model}].EQUIP.COUNT") or 0)
    uid_type, uid_label = {}, {}
    for i in range(n):
        uid = srv.get_value(f"GAP.MOD[{model}].EQUIP[{i}].UniqueID")
        etype = srv.get_value(f"GAP.MOD[{model}].EQUIP[{i}].Type")
        label = srv.get_value(f"GAP.MOD[{model}].EQUIP[{i}].Label")
        if uid:
            uid_type[uid] = (etype.upper() if etype else "UNKNOWN")
            uid_label[uid] = label or uid
    return uid_type, uid_label


def get_all_edges_with_uids(srv, model="PROD", equip_types=TWO_PORT_TYPES):
    """
    Collect all 2-port equipments with EndA/EndB UIDs.
    Returns: {equip_uid: (enda_uid, endb_uid, type)}
    """
    edges = {}
    for etype in equip_types:
        count = int(srv.get_value(f"GAP.MOD[{model}].{etype}.COUNT") or 0)
        for i in range(count):
            uid   = srv.get_value(f"GAP.MOD[{model}].{etype}[{i}].UniqueID")
            enda  = srv.get_value(f"GAP.MOD[{model}].{etype}[{i}].EndA.UniqueID")
            endb  = srv.get_value(f"GAP.MOD[{model}].{etype}[{i}].EndB.UniqueID")
            if uid and enda and endb:
                edges[uid] = (enda, endb, etype)
    return edges


def build_directed_graph(edges):
    """
    Build directed adjacency: EndA -> EndB
    """
    graph = defaultdict(list)
    for eq_uid, (enda, endb, etype) in edges.items():
        graph[enda].append((eq_uid, endb, etype))  # only A→B
    return graph


# ===================== Branches =====================

def find_trunks_and_branches(edges, uid_type):
    """
    Identify trunklines (always open) and branch pipes (toggle).
    Returns: (trunks, branches)
    - trunks: set of pipe_uids
    - branches: {branch_point_uid: [pipe_uids]}
    """
    graph = build_directed_graph(edges)
    trunks = set()
    branches = defaultdict(list)

    wells = [u for u, t in uid_type.items() if t == "WELL"]
    visited = set()

    def dfs(node):
        if node in visited:
            return
        visited.add(node)
        outs = graph.get(node, [])
        if len(outs) == 1:
            # Single continuation → trunk
            pipe_uid, endb, etype = outs[0]
            trunks.add(pipe_uid)
            dfs(endb)
        elif len(outs) > 1:
            # Branch point → multiple pipes
            for (pipe_uid, endb, etype) in outs:
                branches[node].append(pipe_uid)
                dfs(endb)

    for w in wells:
        dfs(w)

    return trunks, branches


# ===================== Routes =====================

def find_paths_from_well_to_sep(graph, uid_type):
    """
    Find Well → Separator routes following EndA -> EndB direction only.
    Returns: {well_uid: [[path_uids], ...]}
    """
    results = {}
    wells = [u for u, t in uid_type.items() if t == "WELL"]
    seps  = {u for u, t in uid_type.items() if t == "SEP"}

    def dfs(node, path, visited):
        if node in seps:
            return [path]
        paths = []
        for (eq_uid, neigh_uid, etype) in graph.get(node, []):
            if neigh_uid not in visited:
                new_paths = dfs(neigh_uid, path + [eq_uid, neigh_uid], visited | {node})
                paths.extend(new_paths)
        return paths

    for w in wells:
        results[w] = dfs(w, [w], {w})
    return results


# ===================== Unified extractor =====================

def extract_topology(srv, model="PROD"):
    """
    Extract branches + routes from GAP model into a single dict.
    """
    # Step 1: collect
    uid_type, uid_label = get_uid_maps(srv, model)
    edges = get_all_edges_with_uids(srv, model)
    graph = build_directed_graph(edges)

    # Step 2: branches
    trunks, branches = find_trunks_and_branches(edges, uid_type)
    trunks_data = []
    for uid in trunks:
        masked = bool(int(srv.get_value(f"GAP.MOD[{model}].EQUIP[{uid}].ISMASKED") or 0))
        trunks_data.append({
            "uid": uid,
            "type": uid_type.get(uid, "?"),
            "label": uid_label.get(uid, uid),
            "initial_masked": masked
        })

    # Step 3: routes
    routes = find_paths_from_well_to_sep(graph, uid_type)

    # Step 4: build JSON-friendly structure
    data = {
        "model": model,
        "trunks": trunks_data,
        "branches": {
            bp: [
                {"uid": uid, "type": uid_type.get(uid, "?"), "label": uid_label.get(uid, uid)}
                for uid in branch_list
            ]
            for bp, branch_list in branches.items()
        },
        "routes": routes,
        "routes_named": {
            well_uid: [
                [
                    {"uid": uid, "type": uid_type.get(uid, "?"), "label": uid_label.get(uid, uid)}
                    for uid in path
                ]
                for path in paths
            ]
            for well_uid, paths in routes.items()
        },
    }

    return data


# ===================== Save/Load helpers =====================

def save_topology_json(data, path="topology.json"):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"✅ Topology saved to {path}")


def load_topology_json(path="topology.json"):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


import itertools

# ===================== Apply lineup =====================

def apply_lineup(
    srv, 
    topology, 
    chosen_branches, 
    force_unmask_trunks=True, 
    locked_trunks=None
):
    """
    Apply lineup:
    - Trunks:
        * If force_unmask_trunks=True → all trunks unmasked, except those in locked_trunks
        * If False → restore trunks to their initial mask state, unless overridden by locked_trunks
        * If trunk UID in locked_trunks → always MASK
    - Branches:
        * At each branch point → unmask chosen pipe, mask others
    """
    if locked_trunks is None:
        locked_trunks = []

    for trunk in topology["trunks"]:
        if trunk["type"] in ("PIPE", "INLCHK", "INLGEN"):
            uid = trunk["uid"]
            if uid in locked_trunks:
                # Force closed
                srv.do_cmd(f"GAP.MOD[{{PROD}}].EQUIP[{uid}].MASK()")
                continue

            if force_unmask_trunks:
                srv.do_cmd(f"GAP.MOD[{{PROD}}].EQUIP[{uid}].UNMASK()")
            else:
                if trunk.get("initial_masked", False):
                    srv.do_cmd(f"GAP.MOD[{{PROD}}].EQUIP[{uid}].MASK()")
                else:
                    srv.do_cmd(f"GAP.MOD[{{PROD}}].EQUIP[{uid}].UNMASK()")

    # Apply branch decisions
    for bp, pipes in topology["branches"].items():
        for node in pipes:
            if node["type"] in ("PIPE", "INLCHK", "INLGEN"):
                if bp in chosen_branches and node["uid"] == chosen_branches[bp]["uid"]:
                    srv.do_cmd(f"GAP.MOD[{{PROD}}].EQUIP[{node['uid']}].UNMASK()")
                else:
                    srv.do_cmd(f"GAP.MOD[{{PROD}}].EQUIP[{node['uid']}].MASK()")



# ===================== Evaluate objective =====================

def evaluate_lineup(srv):
    """
    Run solver and return total oil rate.
    """
    srv.do_cmd("GAP.SOLVENETWORK()")
    sep_count = int(srv.get_value("GAP.MOD[{PROD}].SEP.COUNT"))
    total_oil = 0.0
    for i in range(sep_count):
        oil = srv.get_value(f"GAP.MOD[{{PROD}}].SEP[{i}].SolverResults[0].OilRate")
        total_oil += float(oil or 0)
    return total_oil


# ===================== Brute-force optimizer =====================

def optimize_lineup_bruteforce(srv, topology, force_unmask_trunks=True, locked_trunks = None):
    """
    Brute-force optimizer across all branch points.
    """
    branch_points = list(topology["branches"].keys())
    choices = [topology["branches"][bp] for bp in branch_points]

    best_score = -1
    best_lineup = None

    for combo in itertools.product(*choices):
        chosen_branches = {branch_points[i]: combo[i] for i in range(len(branch_points))}
        apply_lineup(srv, topology, chosen_branches, force_unmask_trunks, locked_trunks)
        score = evaluate_lineup(srv)
        if score > best_score:
            best_score = score
            best_lineup = chosen_branches
            print(f"➡️ New best {best_score:.2f}")
            for bp, pipe in best_lineup.items():
                print(f"   Branch {bp}: {pipe['label']} ({pipe['uid']})")

    return best_lineup, best_score


# ===================== Greedy optimizer =====================

def optimize_lineup_greedy(srv, topology, force_unmask_trunks=True, locked_trunks = None):
    """
    Greedy optimizer: fix one branch at a time, keep the best.
    """
    branch_points = list(topology["branches"].keys())
    chosen_branches = {}

    print("⚡ Starting greedy optimization...")

    for bp in branch_points:
        best_score = -1
        best_choice = None

        for pipe in topology["branches"][bp]:
            trial_choice = chosen_branches.copy()
            trial_choice[bp] = pipe
            apply_lineup(srv, topology, trial_choice, force_unmask_trunks, locked_trunks)
            score = evaluate_lineup(srv)

            if score > best_score:
                best_score = score
                best_choice = pipe

        chosen_branches[bp] = best_choice
        print(f"✅ Fixed branch {bp}: {best_choice['label']} ({best_choice['uid']}) with score {best_score:.2f}")

    # Final evaluation
    apply_lineup(srv, topology, trial_choice, force_unmask_trunks, locked_trunks)
    final_score = evaluate_lineup(srv)

    print("\n✅ Greedy lineup complete:")
    for bp, pipe in chosen_branches.items():
        print(f"  Branch {bp}: {pipe['label']} ({pipe['uid']})")
    print(f"Total Oil Rate = {final_score:.2f}")

    return chosen_branches, final_score
