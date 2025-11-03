"""Project constant values"""


from enum import Enum


class Const(Enum):
    pass


NAME_MAPPING = {
    "OilCpcOut": {
        "instance_name": "OIL_CPC_OUT",
        "property_name": "Oil Rate"
    },
    "GasOgpOut": {
        "instance_name": "GAS_OGP_OUT",
        "property_name": "Gas Rate"
    },
    "GasInjectionOut": {
        "instance_name": "GAS_INJECTION_OUT",
        "property_name": "Gas Rate"
    },
    "OilMrOut": {
        "instance_name": "OIL_MR_OUT",
        "property_name": "Oil Rate"
    },
    "OilOgpOut": {
        "instance_name": "OIL_OGP_OUT",
        "property_name": "Oil Rate"
    },
    "GasKpcOut": {
        "instance_name": "GAS_KPC_OUT",
        "property_name": "Gas Rate"
    },
    "GasUn2MpOut": {
        "instance_name": "GAS_UN2_MP_OUT",
        "property_name": "Gas Rate"
    },
    "GasUn3MpOut": {
        "instance_name": "GAS_UN3_MP_OUT",
        "property_name": "Gas Rate"
    },
    "OilKpcMpOut": {
        "instance_name": "OIL_KPC_MP_OUT",
        "property_name": "Oil Rate"
    },
    "OilUn2Out": {
        "instance_name": "OIL_UN2_OUT",
        "property_name": "Oil Rate"
    },
    "OilUn3Out": {
        "instance_name": "OIL_UN3_OUT",
        "property_name": "Oil Rate"
    },
    "GasKpcPotOut": {
        "instance_name": "GAS_KPC_POT_OUT",
        "property_name": "Gas Rate"
    },
    "GasUn2PotOut": {
        "instance_name": "GAS_UN2_POT_OUT",
        "property_name": "Gas Rate"
    },
    "OilUn2PotOut": {
        "instance_name": "OIL_UN2_POT_OUT",
        "property_name": "Oil Rate"
    },
    "GasUn3PotOut": {
        "instance_name": "GAS_UN3_POT_OUT",
        "property_name": "Gas Rate"
    },
    "OilUn3PotOut": {
        "instance_name": "OIL_UN3_POT_OUT",
        "property_name": "Oil Rate"
    },
    "GorKpcOut": {
        "instance_name": "GOR_KPC_OUT",
        "property_name": "Gas Oil Ratio"
    },
    "OilCpc%Out": {
        "instance_name": "OIL_CPC_PERCENT_OUT",
        "property_name": "Oil Rate"
    },
    "GasOgp%Out": {
        "instance_name": "GAS_OGP_PERCENT_OUT",
        "property_name": "Gas Rate"
    },
    "GasInjection%Out": {
        "instance_name": "GAS_INJECTION_PERCENT_OUT",
        "property_name": "Gas Rate"
    },
    "OilMr%Out": {
        "instance_name": "OIL_MR_PERCENT_OUT",
        "property_name": "Oil Rate"
    },
    "OilOgp%Out": {
        "instance_name": "OIL_OGP_PERCENT_OUT",
        "property_name": "Oil Rate"
    },
    "GorUn2Out": {
        "instance_name": "GOR_UN2_OUT",
        "property_name": "Gas Oil Ratio"
    },
    "GorUn3Out": {
        "instance_name": "GOR_UN3_OUT",
        "property_name": "Gas Oil Ratio"
    },
    "GasKpcInjectionOut": {
        "instance_name": "GAS_KPC_INJECTION_OUT",
        "property_name": "Gas Rate"
    },
    "GasKpcUn2Out": {
        "instance_name": "GAS_KPC_UN2_OUT",
        "property_name": "Gas Rate"
    },
    "GasKpcUn3Out": {
        "instance_name": "GAS_KPC_UN3_OUT",
        "property_name": "Gas Rate"
    },
    "GasUn2Un3Out#": {
        "instance_name": "GAS_UN2_UN3_OUT",
        "property_name": "Gas Rate"
    },
    "GasUn2KpcOut": {
        "instance_name": "GAS_UN2_DIRECT_KPC_OUT",
        "property_name": "Gas Rate"
    },
    "GasUn3KpcOut": {
        "instance_name": "GAS_UN3_CPC_OUT",
        "property_name": "Gas Rate"
    },
    "GasKpcLpWellIn": {
        "instance_name": "GAS_KPC_LP_WELL_IN",
        "property_name": "Gas Rate"
    },
    "GasKpcMpWellIn": {
        "instance_name": "GAS_KPC_MP_WELL_IN",
        "property_name": "Gas Rate"
    },
    "GasUn2WellIn": {
        "instance_name": "GAS_UN2_WELL_IN",
        "property_name": "Gas Rate"
    },
    "GasUn3WellIn": {
        "instance_name": "GAS_UN3_WELL_IN",
        "property_name": "Gas Rate"
    },
    "OilUn3KpcOut": {
        "instance_name": "OIL_UN3_CPC_OUT",
        "property_name": "Oil Rate"
    },
    "OilTotalOut": {
        "instance_name": "OIL_TOTAL_OUT",
        "property_name": "Oil Rate"
    },
    "GasTotalOut": {
        "instance_name": "GAS_TOTAL_OUT",
        "property_name": "Gas Rate"
    },
    "OilUn2AllocatedOut": {
        "instance_name": "OIL_UN2_ALLOCATED_OUT",
        "property_name": "Oil Rate"
    },
    "OilUn3AllocatedOut": {
        "instance_name": "OIL_UN3_ALLOCATED_OUT",
        "property_name": "Oil Rate"
    },
    "OilUn2Un3Out": {
        "instance_name": "OIL_UN2_UN3_OUT",
        "property_name": "Oil Rate"
    },
    "OilUn2KpcOut": {
        "instance_name": "OIL_UN2_KPC_OUT",
        "property_name": "Oil Rate"
    },
    "GasKpcLpOut": {
        "instance_name": "GAS_KPC_LP_OUT",
        "property_name": "Gas Rate"
    },
    "OilKpcLpOut": {
        "instance_name": "OIL_KPC_LP_OUT",
        "property_name": "Oil Rate"
    },
    "KpcOtherLpOut": {
        "instance_name": "KPC_OTHER_LP_OUT",
        "property_name": "Other Rate"
    },
    "GasTotalInjectionOut": {
        "instance_name": "GAS_TOTAL_INJECTION_OUT",
        "property_name": "Gas Rate"
    },
    "GasKpcMpFreeOut": {
        "instance_name": "GAS_KPC_MP_FREE_OUT",
        "property_name": "Gas Rate"
    },
    "OilKpcMpPotOut": {
        "instance_name": "OIL_KPC_MP_POT_OUT",
        "property_name": "Oil Rate"
    },
    "GasKpcMp%Out": {
        "instance_name": "GAS_KPC_MP_PERCENT_OUT",
        "property_name": "Gas Rate"
    },
    "GasKpcTotal%Out": {
        "instance_name": "GAS_KPC_TOTAL_PERCENT_OUT",
        "property_name": "Gas Rate"
    },
    "GasUn2Mp%Out": {
        "instance_name": "GAS_UN2_MP_PERCENT_OUT",
        "property_name": "Gas Rate"
    },
    "GasUn3Mp%Out": {
        "instance_name": "GAS_UN3_MP_PERCENT_OUT",
        "property_name": "Gas Rate"
    },
    "GasUn3OgpFreeOut": {
        "instance_name": "GAS_UN3_OGP_FREE_OUT",
        "property_name": "Gas Rate"
    },
    "OilKpcMpAllocatedOut": {
        "instance_name": "OIL_KPC_MP_ALLOCATED_OUT",
        "property_name": "Oil Rate"
    },
    "KpcMpWellsSgOut": {
        "instance_name": "KPC_MP_WELLS_SG_OUT",
        "property_name": "Other Rate"
    },
    "GasKpcLpPotOut": {
        "instance_name": "GAS_KPC_LP_POT_OUT",
        "property_name": "Gas Rate"
    },
    "U2U3SgAtKpcOut": {
        "instance_name": "U2_U3_SG_AT_KPC_OUT",
        "property_name": "Other Rate"
    },
    "GasFuelInitialIn": {
        "instance_name": "GAS_FUEL_INITIAL_IN",
        "property_name": "Gas Rate"
    }
}
