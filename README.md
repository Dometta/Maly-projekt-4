# Mały-projekt-4

W celu uruchomienie zadania 4, nalezy wpisać następującą komendę będąc w katalogu workflow:
 - snakemake -s Snakefile_task4 --cores 1

## Plik task4.yaml: 
W tym pliku nalezy wpisać odpowiednie parametry, które wykorzystywane będą potem do analizy: 
- years - lata dla który przeprowadzić analizę 
- pm25_norm - dobowa norma stężenia PM2.5 
- entrez_email - email, wykorzystywany do pobrania odpowiednich artykułów z PubMed 
- pubmed_limit - limit artykułów, które chcemy pobrać 
 - batch_size - wielkość paczki

## Opis scenariusza 1
1) Ustawienie w pliku task4.yaml, odpowiednich parametrów:  
- years [2021, 2024] (wymagane)
- reszta parametrów dowolnie
2) Aby uruchomić cały workflow, nalezy wpisać komendę:
- snakemake -s Snakefile_task4 --cores 1

Pipeline policzy:
- PM2.5 dla 2021 i 2024
- Pubmed dla 2021 i 2024
- wspólny raport dla 2021 i 2024

## Opis scenariusza 2
1) Zmiana parametru w pliku task4.yaml:
- years [2019, 2024] - zamiana roku 2021 na rok 2019
2) Ponownie w celu uruchomienia całego workflow, nalezy wpisać komendę:
- snakemake -s Snakefile_task4 --cores 1

Uwaga:
Zgodnie z wymaganiem, rok 2024, nie przelicza się ponownie, poniewaz wszystkie pliki juz istnieja i nic sie w nich nie zmienilo. Mozna to zauwazyć po logach snakemake, a konkretnie w tabelce,
job i count, gdzie wypisywany jest ilość jobów do wykonania. Widać, ze kazda reguła wykonuje się tylko raz, a nie dwukrotnie, co oznacza, 
rok 2024 nie zostaje przeliczany ponownie. Rowniez poźniejsze logi pokazują, ze pliki powstają tylko dla roku 2019.

Pipeline policzy:
- PM2.5 dla 2019
- Pubmed dla 2019
- wspólny raport dla 2019 i 2024
