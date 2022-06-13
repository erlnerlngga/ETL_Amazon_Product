import pandas as pd

def clean_laptop_phone_bed_furniture(df):
    df.drop_duplicates(ignore_index=True, inplace=True)
    
    revi= df.Review_Count.apply(lambda x: True if (len(x)==0) else False) #rating
    idx = list(df.loc[revi, :].index)
    df.loc[idx, 'Ratings'] = '0.0 out of 5 stars'
    df.loc[idx, 'Review_Count'] = '0'
    
    df.Price.replace(',', '' , regex=True, inplace=True)
    df.Review_Count.replace(',', '' , regex=True, inplace=True)
    df.Ratings = df.Ratings.apply(lambda x: x[:3])
    df = df.astype({'Price': 'float', 'Review_Count': 'int', 'Ratings': 'float'})
    return df

def clean_tablet_camera_cook(df):
    df.drop_duplicates(ignore_index=True, inplace=True)
    
    revi= df.Review_Count.apply(lambda x: True if (len(x)==0) else False) 
    rat = df.Ratings.apply(lambda x: True if (len(x)==0) else False) 
    idx_revi = list(df.loc[revi, :].index)
    idx_rat = list(df.loc[rat, :].index)
    
    df.loc[idx_rat, 'Ratings'] = '0.0 out of 5 stars'
    df.loc[idx_revi, 'Review_Count'] = '0'
    
    df.Price.replace(',', '' , regex=True, inplace=True)
    df.Review_Count.replace(',', '' , regex=True, inplace=True)
    df.Ratings = df.Ratings.apply(lambda x: x[:3])
    
    temp_idx = df.Ratings.str.isalpha()
    df.loc[temp_idx, 'Ratings'] = '0.0'
    df = df.astype({'Price': 'float', 'Review_Count': 'int', 'Ratings': 'float'})
                
    return df