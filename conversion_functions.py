

def change_postcode_to_first_elem(df):
    """
    Crop postcode to just the first component
    :param df: dataframe containing postcode variables
    :return:
    """
    postcode_first_comp = []
    for postcode in df['Postcode'].to_list():
        if postcode:
            pc = postcode.replace(' ', '')
            if len(pc) == 6:
                postcode_first_comp.append(pc[:3].upper())
            else:
                postcode_first_comp.append(pc[:4].upper())
        else:
            postcode_first_comp.append(None)
    df.loc[:, 'Postcode'] = postcode_first_comp
    return df
