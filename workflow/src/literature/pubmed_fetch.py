from Bio import Entrez
from Bio import Medline
import pandas as pd
import sys
from pathlib import Path
import yaml
import argparse


def top_words(df,file_path):
    titles = df["Tytuł"].drop_duplicates().fillna("") 
    skips = [".", ", ", ":", ";", "'", '"']  

    list_titles=titles.to_list()   

    for word in skips:
        for i in range(len(list_titles)):
            list_titles[i]=list_titles[i].replace(word,"")

    word_counts={}
    for title in list_titles:
        for word in title.split(" "):
            if len(word) >3: # aby nie łapać takich słów jak is, or, the itd
                if (word.lower() in word_counts):
                    word_counts[word.lower()] +=1
                else:
                    word_counts[word.lower()] =1  
    df_word = (
    pd.DataFrame.from_dict(word_counts, orient="index", columns=["Liczba"])
      .reset_index() # index zamienia się w kolumnę
      .rename(columns={"index": "Słowo"}))
    
    top = df_word.sort_values("Liczba", ascending=False).head(10)
    top.to_csv(file_path,index=False)

    return df_word
    
     


def download_data(email, query, file_path, year, limit, batch_size):

    Entrez.email = email  # Always tell NCBI who you are

    term=f"{query} AND {year} [PDAT]" # który rok wyszukujemy

    stream = Entrez.esearch(db="pubmed", term=term, usehistory="y", retmax=limit)
    search_results = Entrez.read(stream)
    stream.close()

    count = int(search_results["Count"]) # ile artykułów pasuje do zapytania


    webenv = search_results["WebEnv"]
    query_key = search_results["QueryKey"]

    papers=[]

    for start in range(0, min(count,limit), batch_size):
        end = min(count, start + batch_size)
        print("Going to download record %i to %i" % (start + 1, end))
        stream = Entrez.efetch(db="pubmed", rettype="medline", retmode="text",
                            retstart=start, retmax=batch_size, webenv=webenv, query_key=query_key)
        records = Medline.parse(stream)

        for record in records:
            paper ={
                "PMID":record.get("PMID", "?"),
                "Tytuł":record.get("TI", ""),
                "Rok": (record.get("DEP", None)[:4] if record.get("DEP") else None),
                "Journal":record.get("JT", ""), # journal title
                "Autorzy": record.get("AB", "?"),
                "Abstrakt": record.get("SO", "?")
            }

            papers.append(paper)

    df=pd.DataFrame(papers)

    df.to_csv(file_path, index=True)

    return df

def top_10(df,file_path):
    journal_counts = df['Journal'].value_counts(dropna=True)

    top_10=journal_counts.head(10)
    top_10.to_csv(file_path)

    return top_10

def summary_by_year(df, file_path):
    df_clean=df[df["Rok"].notna()]
    summary=df_clean["Rok"].value_counts(dropna=True)
    summary.to_csv(file_path)

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--config", required=True)
    args=parser.parse_args()
    
    # Wczytuję plik config
    config=yaml.safe_load(open(args.config))
    year=args.year
    email=config["entrez_email"]
    limit=config.get("pubmed_limit", 100)
    batch_size=config.get("pubmed_batch_size",50)
    query=config.get("pubmed_query","")
    
    base_path=Path(f"results/literature/{year}")

    file_papers = base_path / "pubmed_papers.csv"
    file_summary = base_path / "summary_by_year.csv"
    file_top_journals = base_path / "top_journals.csv"
    file_top_words = base_path / "top_words.csv"


    df=download_data(email,query,file_papers,year,limit,batch_size)

    top_10(df,file_top_journals)
    summary_by_year(df,file_summary)
    top_words(df,file_top_words)

