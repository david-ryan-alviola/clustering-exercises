import env
import utilities as util
import pandas as pd

_zillow_query = """
SELECT *
	FROM predictions_2017 pred
		JOIN (
			SELECT parcelid, max(transactiondate) AS latest_transaction
				FROM predictions_2017
				GROUP BY parcelid) trans
			ON pred.parcelid = trans.parcelid AND pred.transactiondate = trans.latest_transaction
		JOIN properties_2017 prop ON pred.parcelid = prop.parcelid
		LEFT JOIN airconditioningtype ac ON prop.airconditioningtypeid = ac.airconditioningtypeid
		LEFT JOIN architecturalstyletype ar ON prop.architecturalstyletypeid = ar.architecturalstyletypeid
		LEFT JOIN buildingclasstype bc ON prop.buildingclasstypeid = bc.buildingclasstypeid
		LEFT JOIN heatingorsystemtype ht ON prop.heatingorsystemtypeid = ht.heatingorsystemtypeid
		LEFT JOIN propertylandusetype pl ON prop.propertylandusetypeid = pl.propertylandusetypeid
		LEFT JOIN storytype st ON prop.storytypeid = st.storytypeid
		LEFT JOIN typeconstructiontype ct ON prop.typeconstructiontypeid = ct.typeconstructiontypeid
	WHERE latitude IS NOT NULL
		AND longitude IS NOT NULL
"""

def acquire_zillow():
    return util.generate_df("zillow_clustering.csv", _zillow_query, util.generate_db_url(env.user, env.password, env.host, "zillow"))

def show_missing_value_stats_by_col(df):
    cols = df.columns
    rows = len(df)
    result = pd.DataFrame(index=cols, columns=['num_rows_missing', 'pct_rows_missing'])
    pd.set_option('max_rows', rows)
    
    result['num_rows_missing'] = df.isnull().sum()
    result['pct_rows_missing'] = round(df.isnull().sum() / rows, 6)
    
    return result

def show_missing_value_stats_by_row(df):
    total_cols = df.shape[1]
    total_rows = df.shape[0]
    result = pd.DataFrame(df.isnull().sum(axis=1).value_counts(), columns=['num_rows'])
    pd.set_option('max_rows', total_rows)
    
    result = result.reset_index()
    result = result.rename(columns={'index' : 'num_cols_missing'})
    result['pct_cols_missing'] = result['num_cols_missing'] / total_cols
    result = result.set_index('num_cols_missing')
    result = result.sort_values('num_cols_missing', ascending=True)
    
    return result

def handle_missing_values(df, col_thresh, row_thresh):
    req_col = int(round(col_thresh * len(df.index), 0))
    req_row = int(round(row_thresh * len(df.columns), 0))
    
    df = df.dropna(axis=1, thresh=req_col)
    df = df.dropna(axis=0, thresh=req_row)
    
    return df

def wrangle_zillow_df():
    return handle_missing_values(acquire_zillow(), .5, .5)