import datetime as dt
import pyodbc
import pandas as pd
class LocationAndSales:

    def __init__(self, store_id, lob_id,sales_history,folder_name, distance_threshold, lob_opt_ins):
        self.store_id = store_id
        self.lob_id = lob_id
        self.sales_history = sales_history
        self.folder_name = folder_name
        self.distance_threshold = distance_threshold
        self.lob_opt_ins = lob_opt_ins
        
        import ctypes
        ctypes.windll.user32.SystemParametersInfoW(20, 0, 'G:\Analytics\Data Team\Data Extensions\DataExtensionRequirements.jpg' , 0)
        
        if isinstance(store_id, int):
            self.store_compare = f" = {store_id}"
        elif isinstance(store_id, tuple):
            self.store_compare = f" in {store_id}"

        if isinstance(lob_id, int):
            self.lob_compare = f" = {lob_id}"
        elif isinstance(lob_id, tuple):
            self.lob_compare = f" in {lob_id}"
        
    def GetDistance(self,lat1, lon1, lat2, lon2):
        import sys
        from math import sin, cos, sqrt, atan2, radians

        lat1 = radians(float(lat1))
        lon1 = radians(float(lon1))
        lat2 = radians(float(lat2))
        lon2 = radians(float(lon2))

        # approximate radius of earth in km
        R = 6373.0

        dlon = lon2 - lon1
        dlat = lat2 - lat1

        a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        distance = R * c #miles
        return distance * 1.609 #km's
    def GetStorePostalCode(self,store_id):
        import pandas as pd
        import pyodbc
        conn = pyodbc.connect(r'Driver={SQL SERVER};'
                         'Server=dwhsqlprod\dwhsqlprod;'
                          'Database=dwh_schema;'
                         'Trusted_Connection=yes;')

        query = f"""

        select postalcode
        from dim_store
        where store_id = {store_id}

        """
        results = pd.read_sql_query(query, conn)
        postal_code = str(results.values[0][0])
        return postal_code

    def GetLatLongDF(self):
        import pandas as pd
        df =  pd.read_excel('G:\Analytics\Data Team\Transactional Data\Old PBI Data\LatandLong.xlsx')
        df.columns = 'PostalCode', 'Latitude', 'Longitude'
        return df
    def GetCurrentPostalCodes(self):
        import pandas as pd
        import pyodbc
        conn = pyodbc.connect(r'Driver={SQL SERVER};'
                         'Server=dwhsqlprod\dwhsqlprod;'
                          'Database=dwh_schema;'
                         'Trusted_Connection=yes;')
        query = f"""
        select n.MemberNumber, concat(left(m.postal_code, 3), ' ', right(m.postal_code,3)) as PostalCode
        from dim_member_hist as m
        inner join 

        (select membership_num as MemberNumber, 
            max(Start_date) StartDate
            --concat(left(m.postal_code, 3), ' ', right(m.postal_code,3)) as postal_code

        from dim_member_hist as m
        where membership_status in ('Active', 'Age Policy')
        group by membership_num) as n

        on n.StartDate=m.Start_date and n.MemberNumber=m.membership_num
        """

        return pd.read_sql_query(query, conn)

    def GetPreferences(self):
        import pandas as pd
        df = pd.read_csv('G:\Analytics\Data Team\Data Extensions\Data Extension Downloads\Contact Salesforce\PreferenceFile.csv')
        df = df.loc[~df['Email Address'].isnull()]
        return df
    
    def GetGeoMembers(self):
        import pandas as pd
        df = member_postal_codes.merge(latlong, how='inner', on='PostalCode')
        df['StoreCode'] = store_postal_code
        df = df.merge(latlong, how='inner', left_on='StoreCode', right_on='PostalCode')
        df.columns = ['MemberNumber', 'MemberCode', 'MemberLat', 'MemberLon', 'StoreCode', 'StoreCode2', 'StoreLat', 'StoreLon']
        df =  df[['MemberNumber', 'MemberCode', 'MemberLat', 'MemberLon', 'StoreCode', 'StoreLat', 'StoreLon']]
        df['Distance'] = [GetDistance(df.loc[x,'MemberLat'],df.loc[x,'MemberLon'],df.loc[x,'StoreLat'],df.loc[x,'StoreLon']) for x in df.index]
        df = pd.DataFrame(df.loc[df.Distance <= distance_threshold ]['MemberNumber'])
        return df
    
    def GetSalesMembers(self):
        import pandas as pd
        conn = pyodbc.connect(r'Driver={SQL SERVER};'
                         'Server=dwhsqlprod\dwhsqlprod;'
                          'Database=dwh_schema;'
                         'Trusted_Connection=yes;')
        relative_date = str(dt.datetime.now() - dt.timedelta(days=self.sales_history*30)).split('.')[0]
        query = f"""

        select
           m.membership_num as MemberNumber


        from fact_transline as f with (NOLOCK)

            left join DIM_MEMBER_HIST as m with (NOLOCK)
                ON f.member_id = m.member_id 



        where record_type in (1, 4) 
        and f.transline_datetime >= '{relative_date}'
        and f.lob_id {lob_compare}
        and f.store_id {store_compare}
        group by m.membership_num
        having sum(f.transline_amount) >0

        """

        return pd.read_sql_query(query, conn)

    def GetEmailAddresses(self):
        import pandas as pd
        import pyodbc
        conn = pyodbc.connect(r'Driver={SQL SERVER};'
                         'Server=dwhsqlprod\dwhsqlprod;'
                          'Database=auxiliarydb;'
                         'Trusted_Connection=yes;')
        relative_date = str(dt.datetime.now() - dt.timedelta(days=self.sales_history*30)).split('.')[0]
        query = f"""

        select
           k.MEMBERACCOUNTID as MemberNumber,
           k.EMAILADDRESS as [Email Address]
           from mk_membership as k


        """

        return pd.read_sql_query(query, conn)

    def GetValidMembers(self):
        import pandas as pd
        df = geo_members.append(sales_members).drop_duplicates()
        df = df.merge(emails, how='inner', on='MemberNumber').dropna()   
        df = df.merge(preferences, how='inner', on='Email Address')
        df = df.loc[df.IsEmailStandardInclusion==True]
        if 'Corp' in lob_opt_ins:
            df = df.loc[
                (df.FoodOptIn==True) &
                (df.GasOptIn==True)
            ]
        if 'WSB' in lob_opt_ins:
                df =  df.loc[
                (df.WSBOptIn==True) &
                (df.WSB_Age==True)
            ]
        return df
    def main(self):
        store_postal_code = self.GetStorePostalCode(self.store_id)
        latlong = self.GetLatLongDF()
        member_postal_codes = self.GetCurrentPostalCodes()
        preferences = self.GetPreferences()
        emails = self.GetEmailAddresses()
        geo_members = self.GetGeoMembers()
        sales_members = self.GetSalesMembers()
        final_list = self.GetValidMembers().set_index('MemberNumber')
        final_list.to_csv(self.folder_name)
        return print("This Extension is complete.")
