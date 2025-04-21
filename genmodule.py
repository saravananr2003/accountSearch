
def get_address(pd_addr, pd_dataset, pn_spaces):
    ld_addr = {}
    for rec in pd_dataset:
        # print(rec[0], " _ ", rec[1])
        ls_retattr = rec[0]
        ls_attr = str(rec[1])
        if rec[2] == 0:
            ld_addr[ls_retattr] = str(pd_addr.find(f'ns4:{ls_attr}', pn_spaces).text or "")
            ld_addr[ls_retattr + "_FAULT_CD"] = str(pd_addr.find(f'ns4:{ls_attr}', pn_spaces).get('FaultCode') or "")
            ld_addr[ls_retattr + "_FAULT_DESC"] = str(pd_addr.find(f'ns4:{ls_attr}', pn_spaces).get('FaultDesc') or "")
        else:
            ld_addr[ls_retattr] = str(pd_addr.get(ls_attr) or "")

    return ld_addr

