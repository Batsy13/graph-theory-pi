# Análise de Dados da Câmara dos Deputados com Neo4j

Este projeto visa extrair, modelar e analisar dados públicos da Câmara dos Deputados do Brasil, utilizando um banco de dados de grafo (Neo4j) para mapear e explorar as complexas relações entre deputados, partidos e votações.

## Sobre o Projeto

O objetivo principal é transformar os dados tabulares e distribuídos da API "Dados Abertos" em um grafo conectado, permitindo a execução de consultas complexas e a descoberta de padrões que seriam difíceis de identificar em um modelo relacional tradicional.

## Tecnologias Utilizadas

[![Python][Python]][Python-url]
[![Neo4j][Neo4j]][Neo4j-url]

[Python]: https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54 
[Python-url]: https://www.python.org/
[Neo4j]: https://img.shields.io/badge/Neo4j-008CC1?logo=neo4j&logoColor=white
[Neo4j-url]: https://neo4j.com/

## Começando

Siga estas instruções para obter uma cópia do projeto e executá-la em sua máquina local para desenvolvimento e testes.

### Pré requisitos

Antes de começar, garanta que você possui os seguintes itens instalados na sua máquina:

* **Git**: Para clonar este repositório.
* **Python**: Para poder rodar as injeções.
* **Neo4j Desktop**: Interface para visualização do banco de dados.

Recomendo utilizar um editor de código como [VSCode](https://code.visualstudio.com/) para uma melhor experiência de desenvolvimento.

### Rodando o Projeto

1.  **Clone este repositório:**
    ```bash
    git clone https://github.com/Batsy13/graph-theory-pi
    ```
2.  **Vá para a pasta do projeto:**
    ```bash
    cd graph-theory-pi
    ```
3. **instale as bibliotecas necessárias:**
   ```bash
   pip install neo4j
   ```
   ```bash
   pip install requests
   ```
   ```bash
   pip install dotenv
   ```

4.  **Crie um banco de dados dentro do seu neo4j Desktop ou neo4j Aura**

5.  **Adicione os dados no .env:**
    ```bash
    DATABASE="{nome do banco de dados}"
    PASSWORD="{senha do usuário}"
    URI="{url que está rodando o banco de dados}"
    ```

6. **Rode os códigos de injeção:**
   ```bash
   python3 scripts/deputy_injection.py
   ```
   ```bash
   python3 scripts/party_injection.py
   ```
   ```bash
   python3 scripts/votations_injection.py
   ```
   ```bash
   python3 scripts/propositions_injection.py
   ```
   ```bash
   python3 scripts/organ_injection.py
   ```

7. **Faça uma consulta no neo4j Desktop para visualização dos dados:**
   ```bash
   MATCH (n)
   return n
   ```

---
