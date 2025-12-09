# Hadoop MapReduce Exercises Collection

Este repositório contém uma coleção de exercícios práticos de **Hadoop MapReduce** desenvolvidos em Java. O projeto está dividido em níveis de complexidade, variando de contagens simples de palavras (WordCount) a cadeias de Jobs (Chain Mapper/Reducer) e implementação de serialização customizada (`Custom Writables`) para manipulação de dados complexos.

## 📂 Estrutura do Projeto

O código está organizado em dois pacotes principais:

  * **`basic`**: Contém as classes `Driver` (com `main`), `Mapper` e `Reducer` para resolução de problemas de negócios baseados em transações comerciais (Commodities).
  * **`advanced`**:
      * **`entropy`**: Contém um algoritmo específico para cálculo de Entropia de Shannon em sequências genéticas (FASTA).
      * **`customwritable`**: Contém classes que implementam a interface `Writable` e `WritableComparable` do Hadoop. **Nota:** Estas classes servem de base tanto para os exercícios avançados quanto para os exercícios da pasta `basic`.

-----

## 📊 Datasets Utilizados

Os exercícios foram projetados para processar os seguintes tipos de arquivos (localizados na pasta `in/`):

1.  **`transactions_amostra.csv`**: Dados de comércio exterior (Exportação/Importação) contendo: `country`, `year`, `comm_code`, `commodity`, `flow`, `trade_usd`, `weight_kg`, `quantity_name`, `category`.
2.  **`*.fasta`**: Sequências de DNA para análise de entropia.
3.  **`bible.txt`** ou textos gerais: Para testes simples de WordCount.
4.  **Forest Fire / Weather Data**: Para o exercício de temperatura média.

-----

## 🚀 Lista de Exercícios e Funcionalidades

### 1\. Pacote Basic (Análise de Commodities)

Estes exercícios focam na análise do dataset de transações. Alguns utilizam classes auxiliares do pacote `advanced.customwritable`.

| Classe | Descrição do Problema | Complexidade | Conceitos Chave |
| :--- | :--- | :--- | :--- |
| `Exercicio1` | Contar número de transações envolvendo o "Brazil". | Fácil | Filtro simples (`if`), Count. |
| `Exercicio2` | Número de transações por **Tipo de Fluxo** e **Ano**. | Fácil | Chave Composta (`TipoAnoWritable`). |
| `Exercicio3` | Média dos valores das commodities por **Ano**. | Fácil | Agregação, `ComdAnoValorWritable`. |
| `Exercicio4` | Preço médio por Unidade, Ano e Categoria (Brasil/Export). | Fácil | Filtros múltiplos, Chave complexa (`GroupTipoUnidadeWritable`). |
| `Exercicio5` | Preço Máximo, Mínimo e Médio por Unidade e Ano. | Médio | Objeto de valor complexo (`MaxMinMediaWritable`), Lógica Min/Max no Reducer. |
| `Exercicio6` | País com o maior preço médio de commodity (Exportação). | Difícil | **Job Chaining** (2 Jobs). O 1º calcula médias, o 2º encontra o máximo global. |
| `Exercicio7` | Commodity mais comercializada em 2016 por fluxo. | Difícil | **Job Chaining**. O 1º soma quantidades, o 2º compara totais por fluxo. |
| `Teste` | Prova de conceito simples. | Intro | MapReduce "Hello World". |
| `WordCount` | Esqueleto clássico de contagem de palavras. | Intro | Template base. |

### 2\. Pacote Advanced (Entropia e Customização)

Focado em serialização eficiente e algoritmos científicos.

  * **`EntropyFASTA.java`**: Calcula a Entropia de Shannon de uma sequência de DNA.
      * *Etapa 1:* Conta a frequência de cada base (A, C, T, G) e o total.
      * *Etapa 2:* Calcula $H(X) = - \sum P(x) \log_2 P(x)$.
  * **`AverageTemperature.java`**: Calcula temperatura média utilizando `FireAvgTempWritable` para trafegar somas parciais e contadores do Map para o Reduce.

### 3\. Classes Writable Customizadas (`advanced.customwritable`)

Estas classes permitem que o Hadoop trafegue objetos complexos pela rede e realize ordenações compostas.

  * **Chaves Compostas (Implementam `WritableComparable`):**
      * `TipoAnoWritable`: (Ano, Fluxo)
      * `GroupTipoUnidadeWritable`: (Ano, Commodity, Unidade, Categoria)
      * `ComdAnoValorWritable`: (Commodity, Ano)
      * `GroupMaxMinMedia`: (Ano, Unidade)
      * `PaisMediaWritable`, `ComdTipoFluxoWritable`, etc.
  * **Valores Complexos (Implementam `Writable`):**
      * `MaxMinMediaWritable`: Armazena (N, Soma, Max, Min).
      * `TipoUnidadeWritable`: Armazena (N, Preço).
      * `FireAvgTempWritable`: Armazena (SomaTemperaturas, Qtd).

-----

## 🛠️ Como Compilar e Executar

Certifique-se de ter o Hadoop instalado e configurado.

1.  **Compilação:**

    ```bash
    # Crie uma pasta para as classes compiladas
    mkdir -p classes

    # Compile o código (ajuste o classpath conforme sua instalação do Hadoop)
    javac -cp $(hadoop classpath) -d classes src/basic/*.java src/advanced/customwritable/*.java src/advanced/entropy/*.java
    ```

2.  **Empacotamento (.jar):**

    ```bash
    jar -cvf mapreduce-exercises.jar -C classes/ .
    ```

3.  **Execução:**
    Exemplo para rodar o **Exercicio 4**:

    ```bash
    # Limpe a saída anterior se existir
    hdfs dfs -rm -r output/ex4.txt

    # Execute o Job
    hadoop jar mapreduce-exercises.jar basic.Exercicio4
    ```

    Exemplo para rodar o **EntropyFASTA** (requer argumentos de entrada/saída):

    ```bash
    hadoop jar mapreduce-exercises.jar advanced.entropy.EntropyFASTA in/amostra.fasta output/entropia_result
    ```

-----

## 📝 Notas de Implementação

  * **Job Chaining (Exercícios 6 e 7):** Estes exercícios utilizam arquivos temporários (`intermediate.tmp`) para passar o resultado do primeiro MapReduce para o segundo. O código gerencia caminhos intermediários automaticamente.
  * **Combiners:** Vários exercícios (`Exercicio1`, `Exercicio2`, `AverageTemperature`, etc.) implementam *Combiners* para otimizar a largura de banda da rede, realizando pré-agregações locais antes do envio ao Reducer.
