import pandas as pd
import requests
import zipfile
import io
from pathlib import Path
import sys

# Stałe
META_URL_1 = "https://powietrze.gios.gov.pl/pjp/archives/downloadFile/622"
META_URL_2= "https://powietrze.gios.gov.pl/pjp/archives/downloadFile/584"
GIOS_ARCHIVE_URL = "https://powietrze.gios.gov.pl/pjp/archives/downloadFile/"

GIOS_ID = {
    2015: '236', 
    2018: '603',
    2019: '322', 
    2021: '486', 
    2024: '582'}


GIOS_PM25_FILE = {
    2015: '2015_PM25_1g.xlsx', 
    2018: '2018_PM25_1g.xlsx', 
    2019: '2019_PM25_1g.xlsx',
    2021: '2021_PM25_1g.xlsx', 
    2024: '2024_PM25_1g.xlsx'}


# Pobieranie archiwum GIOŚ
def download_gios_archive(year):
    url = f"{GIOS_ARCHIVE_URL}{GIOS_ID[year]}"
    response = requests.get(url)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        with z.open(GIOS_PM25_FILE[year]) as f:
            df = pd.read_excel(f, header=None)

    return df

def clean_data(df, year):
    # Ustaw pierwszy wiersz jako indeks
    df = df.set_index(0)
    
    # Usuwanie niepotrzebnych wierszy w zależności od roku
    if year == 2015:
        df = df.drop(['Wskaźnik', 'Czas uśredniania'])
    elif year == 2018:
        df = df.drop(['Nr','Wskaźnik','Czas uśredniania', 'Jednostka', 'Czas pomiaru'], axis=0)
    else: 
        df = df.drop(['Nr','Wskaźnik','Czas uśredniania', 'Jednostka', 'Kod stanowiska'], axis=0)
    
    # Ustawienie nagłówków kolumn
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    
    # Konwersja indeksu na datetime
    df.index = pd.to_datetime(df.index)

    # Zaokrąglenie do sekund
    df.index = df.index.round('s')

    # Poprawne przesunięcie pomiarów o północy
    mask = df.index.hour == 0
    shifted_index = (df.index - pd.Timedelta(days=1)).normalize() + pd.Timedelta(hours=23, minutes=59, seconds=59)
    
    # Podmiana tylko dla wierszy z godziny 00:XX:XX
    new_index = df.index.to_series()
    new_index[mask] = shifted_index[mask]
    df.index = pd.DatetimeIndex(new_index)
    
    df.index.name = "Data poboru danych"
    
    return df


# Metadane
def download_metadata(year):
    # Pobieranie z URL:
    if year ==2019:
        '''
        response = requests.get(META_URL_2)
        response.raise_for_status()
        with open("metadane.xlsx", "wb") as f:
            f.write(response.content)
        '''
        metadane = pd.read_excel("metadane.xlsx")
        metadane = metadane.rename(columns={"Stary Kod stacji \n(o ile inny od aktualnego)": "Stary kod"})
        metadane['Stary kod'] = metadane['Stary kod'].str.split(', ')
        metadane = metadane.explode('Stary kod')
        return metadane
    else:
        response = requests.get(META_URL_1)
        response.raise_for_status()
        with open("metadane_new.xlsx", "wb") as f:
            f.write(response.content)
        metadane = pd.read_excel("metadane_new.xlsx")
        metadane = metadane.rename(columns={"Stary Kod stacji \n(o ile inny od aktualnego)": "Stary kod"})
        metadane['Stary kod'] = metadane['Stary kod'].str.split(', ')
        metadane = metadane.explode('Stary kod')
        return metadane

# Mapowanie kodów stacji
def map_station_codes(df, mapping_dict):
    df.columns = df.columns.map(lambda x: mapping_dict.get(x, x))
    return df


# MultiIndex (Kod stacji, Miejscowość)
def make_multi_index(metadane, common_stations):
    common_stations = [st.strip() for st in common_stations]

    filtered = metadane[metadane['Kod stacji'].isin(common_stations)]
    mapping_dict = dict(
        zip(filtered['Kod stacji'], filtered['Miejscowość'])
    )

    station_city = [
        (st_code, mapping_dict.get(st_code, "Nieznana"))
        for st_code in common_stations
    ]


    return pd.MultiIndex.from_tuples(
        station_city,
        names=['Kod stacji', 'Miejscowość']
    )

def prepare_single_year(year, file_path):
    # Pobranie metadanych
    metadane = download_metadata(year)
    mapping_dict = dict(zip(metadane['Stary kod'], metadane['Kod stacji']))
    
    # Pobranie i czyszczenie danych
    df = download_gios_archive(year)
    df = clean_data(df, year)
    
    # Mapowanie kodów stacji
    df = map_station_codes(df, mapping_dict)
    
    # Tworzenie MultiIndex
    multi_index = make_multi_index(metadane, df.columns)
    df.columns = multi_index
    
    # Zapis do CSV
    df.to_csv(file_path, index=True)
    
    return df

if __name__ == "__main__":
    year=int(sys.argv[2])
    file_path = Path(sys.argv[1])
    file_path.parent.mkdir(parents=True, exist_ok=True)
    prepare_single_year(year,file_path)

