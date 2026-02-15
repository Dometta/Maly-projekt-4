import pandas as pd
import sys
from pathlib import Path

PM25_NORM=15 # µg/m3 (dobowa)


def monthly_mean(df,years):
    df_month = (
    df
    .groupby([df.index.year, df.index.month])
    .mean())
    df_years=df_month.loc[years]
    df_years.index.names = ["Rok", "Miesiąc"]

    return df_years

def daily_mean(df):
    df_copy = df.copy()
    
    # Dodaj kolumnę z datą
    df_copy['data'] = df_copy.index.date
    
    df_daily = df_copy.groupby('data').mean()

    return df_daily



def days_above_norm(df, norm, file_path):
    
    df_daily = daily_mean(df)
    
    # Sprawdzenie przekroczeń
    exceed = df_daily > norm
    
    # Grupowanie po roku (tylko istniejące lata)
    days_exceeded= exceed.groupby(pd.DatetimeIndex(df_daily.index).year).sum()
    days_exceeded.index.name = 'Rok'

    days_exceeded.to_csv(file_path)

    return days_exceeded

if __name__ == "__main__":
    df_path=sys.argv[1]
    pm25_norm=int(sys.argv[2])
    output_path_exceed=Path(sys.argv[3])
    output_path_mean=Path(sys.argv[4])

    df=pd.read_csv(
        df_path,
        header=[0,1], # zachowuje multiindex
        index_col=0, 
        parse_dates=True
    )

    df_mean=daily_mean(df)
    df_mean.to_csv(output_path_mean)
    days_above_norm(df, pm25_norm, output_path_exceed)





