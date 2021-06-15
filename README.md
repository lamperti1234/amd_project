Progetto per il corso Algorithms for Massive Dataset dell'Università degli Studi Statale di Milano 2020/21


# HOW TO DOWNLOAD FROM KAGGLE
Per scaricare un dataset da Kaggle bisogna innanzitutto:
* specificare in un file `config.json` (nella cartella `conf`) il nome del dataset da scaricare
`DATASET: <autore>/<nome dataset>`
* specificare le credenziali di kaggle in un file `kaggle.json` (nell cartella `conf`). Richiede l'username e la key

Una volta configurato correttamente, basta utilizzare la funzione `download_dataset`