Progetto per il corso Algorithms for Massive Dataset dell'Università degli Studi Statale di Milano 2020/21

# Credenziali Kaggle

Al fine di scaricare un dataset da Kaggle è necessario specificare, in un file `kaggle.json` le credenziali necessarie:

- `username: <value>`
- `key: <value>`

Per ottenere le credenziali:

- loggarsi con il proprio account su [Kaggle](https://www.kaggle.com/)
- andare su `Your profile`
- andare su `Account`
- creare le credenziali cliccando su `Create New API Token`

# Config file

E' presente un file `config.json` (nella cartella `conf`) che consente di settare alcuni parametri utilizzati nel
progetto.

## Kaggle

- dataset: si può specificare il dataset che deve essere scaricato e successivamente utilizzato. Il valore deve essere
  specificato come `DATASET: <autore>/<nome dataset>`

## Parametri algoritmi

- soglia: per l'algoritmo `APRIORI` è possibile settare un arbitrario valore di soglia Il valore deve essere specificato
  come `THRESHOLD: <intero>`
- chunk: per l'algoritmo `SON` è possibile settare un arbitrario valore di chunk in cui suddividere il file. Il valore
  deve essere specificato come `SON_CHUNKS: <intero>`
- massimo numeri di iterazioni: per l'algoritmo `TOIVONEN` è possibile settare un arbitrario valore per il numero
  massimo di iterazioni prima di rinunciare all'esecuzione. L'algoritmo infatti prevede la possibilità di non terminare
  correttamente ma è possibile ritentare con un altro sample. Il valore deve essere specificato
  come `TOIVONEN_MAX_ITERATIONS: <intero>`
- grandezza del sample: per l'algoritmo `TOIVONEN` è possibile settare la grandezza del sample da usare. Il valore deve
  essere specificato come `TOIVONEN_SIZE_SAMPLE: <intero>`
- aggiustamento della soglia: per l'algoritmo `TOIVONEN` è possibile settare di quanto abbassare la soglia (oltre a
  farla diventa p*s) per evitare falsi negativi. Il valore deve essere specificato
  come `TOIVONEN_THRESHOLD_ADJUST: <float>`

## Manipolazione dei risultati

- save: è possibile specificare se si vogliono salvare i frequent itemsets che sono stati scoperti dall'algoritmo
  scelto. A seconda dell'algoritmo i dati saranno salvati in `parquet` (con `apriori_spark`) oppure 'csv' (con i
  restanti algoritmi). Il valore deve essere specificato come `SAVE: <boolean>`
- dump: è possibile visualizzare alcuni dati di interesse per i frequent itemset sottoforma di dataframe Spark. I dati
  includono le prime 20 righe (ordinate per supporti dei frequent itemset) e valori statistici quali count, media,
  massimo e minimo. Il valore deve essere specificato come `DUMP: <boolean>`

## Log

- livello: è possibile specificare fino a quale livello si vogliono visualizzare i messaggi di log. Il valore deve
  essere sepcificato come `LOG_LEVEL: <livello>` dove livello può essere: DEBUG / INFO / WARNING / ERROR
- formato: è possibile specificato il formato dei messaggi di log. Il valore deve essere specificato
  come `LOG_FORMAT: <format>`