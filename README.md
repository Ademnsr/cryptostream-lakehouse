# CryptoStream Lakehouse

A small data pipeline that takes live crypto trades from Coinbase and turns
them into clean, query ready tables. Built as a portfolio project, not a
trading bot, it does not try to predict prices.

[English](#english) | [Français](#français)

## English

### What it does

The pipeline connects to Coinbase's public WebSocket, reads trades for
BTC-USD, ETH-USD and SOL-USD in real time, and sends them through Kafka into
S3. From there, Athena and dbt turn the raw data into a clean table (Silver)
and into minute and daily metrics like OHLCV and VWAP (Gold).

### Pipeline

```
Coinbase WebSocket -> Python producer -> Kafka -> Kafka Connect (S3 sink) -> S3 (Bronze)
S3 (Bronze) -> Athena (Glue catalog) -> dbt-athena -> Silver and Gold tables
```

### Stack

- Python (websockets, aiokafka) for the producer
- Apache Kafka
- Kafka Connect with the S3 sink connector
- AWS S3, Glue, Athena
- dbt-athena for the transformations
- Docker Compose to run everything locally
- CloudFormation for the AWS side

### Repo structure

- `producer/`: connects to Coinbase, validates and normalizes trades, sends them to Kafka
- `kafka-connect/`: config for the S3 sink connector, plus a write up of a crash and recovery test (`RESILIENCE.md`)
- `infrastructure/cloudformation/`: the AWS stack (S3 bucket, Glue databases, Bronze table, Athena workgroup, IAM users)
- `dbt/`: staging, intermediate and mart models that build Silver and Gold
- `scripts/`: small helper scripts (create the topics, register the connector)
- `queries/`: example Athena queries on the Bronze table
- `tests/`: unit tests for the producer

### Running it locally

1. Deploy the AWS stack, see `infrastructure/cloudformation/README.md`. This creates the S3 bucket, the Glue databases and the IAM users.
2. Copy `.env.example` to `.env` and fill in the AWS credentials and the S3 bucket name.
3. Start Kafka and Kafka Connect:
   ```
   docker compose up -d kafka kafka-connect
   ```
4. Create the topics and register the S3 sink connector:
   ```
   bash scripts/create-topics.sh
   bash scripts/register-connector.sh
   ```
5. Run the producer:
   ```
   python -m producer.main
   ```
6. Once trades are landing in S3, build the Silver and Gold models:
   ```
   docker compose run --rm dbt build
   ```

### Some of the choices made along the way

- The producer uses `acks=all` and idempotence on the Kafka side, so a trade can't get lost or sent twice by the producer itself.
- Prices and sizes stay as text until dbt casts them to `decimal(20,8)`. Floats round crypto amounts in a way that is hard to trust.
- Trades that fail validation (bad price, missing field, etc.) go to a separate dead letter topic instead of crashing the producer.
- The S3 sink writes a file every 1000 records or every 5 minutes, whichever comes first, and only moves its Kafka offset forward once a file is actually saved to S3. That is what makes the crash test in `kafka-connect/RESILIENCE.md` work: a crash can't lose data or silently duplicate it.
- The Bronze table in Athena uses partition projection instead of a Glue crawler. It is cheaper and there is nothing to run on a schedule.
- Duplicates (Kafka can redeliver a message) are removed in dbt by keeping the latest ingested copy of each `event_id`, not in the producer.
- IAM is split into two users: one that can only write into the Bronze S3 prefix (Kafka Connect), one that can read Bronze and read/write Silver and Gold (dbt). Neither can touch the other's data.

### Tests

- `tests/`: pytest unit tests for trade validation and the Kafka client
- `dbt/tests/`: schema tests (not null, unique, accepted values) plus two custom tests checking that price and size are always positive
- `kafka-connect/RESILIENCE.md`: a real crash and recovery test on the S3 sink connector

---

## Français

### Ce que ça fait

Le pipeline se connecte au WebSocket public de Coinbase, lit les trades sur
BTC-USD, ETH-USD et SOL-USD en temps réel, et les envoie via Kafka jusqu'à
S3. Ensuite, Athena et dbt transforment les données brutes en une table
propre (Silver) et en métriques par minute et par jour comme l'OHLCV et le
VWAP (Gold).

### Pipeline

```
Coinbase WebSocket -> producteur Python -> Kafka -> Kafka Connect (S3 sink) -> S3 (Bronze)
S3 (Bronze) -> Athena (catalogue Glue) -> dbt-athena -> tables Silver et Gold
```

### Stack technique

- Python (websockets, aiokafka) pour le producteur
- Apache Kafka
- Kafka Connect avec le connecteur S3 sink
- AWS S3, Glue, Athena
- dbt-athena pour les transformations
- Docker Compose pour tout lancer en local
- CloudFormation pour la partie AWS

### Structure du repo

- `producer/`: se connecte à Coinbase, valide et normalise les trades, les envoie vers Kafka
- `kafka-connect/`: config du connecteur S3 sink, et un compte rendu d'un test de crash et récupération (`RESILIENCE.md`)
- `infrastructure/cloudformation/`: la stack AWS (bucket S3, bases Glue, table Bronze, workgroup Athena, utilisateurs IAM)
- `dbt/`: modèles staging, intermediate et marts qui construisent Silver et Gold
- `scripts/`: petits scripts utilitaires (création des topics, enregistrement du connecteur)
- `queries/`: exemples de requêtes Athena sur la table Bronze
- `tests/`: tests unitaires du producteur

### Le lancer en local

1. Déployer la stack AWS, voir `infrastructure/cloudformation/README.md`. Ça crée le bucket S3, les bases Glue et les utilisateurs IAM.
2. Copier `.env.example` en `.env` et remplir les identifiants AWS et le nom du bucket S3.
3. Démarrer Kafka et Kafka Connect :
   ```
   docker compose up -d kafka kafka-connect
   ```
4. Créer les topics et enregistrer le connecteur S3 sink :
   ```
   bash scripts/create-topics.sh
   bash scripts/register-connector.sh
   ```
5. Lancer le producteur :
   ```
   python -m producer.main
   ```
6. Une fois que les trades arrivent dans S3, construire les modèles Silver et Gold :
   ```
   docker compose run --rm dbt build
   ```

### Quelques choix faits pendant le projet

- Le producteur utilise `acks=all` et l'idempotence côté Kafka, pour qu'un trade ne puisse pas être perdu ou envoyé deux fois par le producteur lui même.
- Les prix et tailles restent en texte jusqu'à ce que dbt les convertisse en `decimal(20,8)`. Les floats arrondissent les montants crypto d'une façon qu'on ne peut pas vraiment faire confiance.
- Les trades qui ratent la validation (prix invalide, champ manquant, etc.) partent dans un topic séparé (dead letter) au lieu de faire planter le producteur.
- Le sink S3 écrit un fichier tous les 1000 messages ou toutes les 5 minutes, selon ce qui arrive en premier, et n'avance son offset Kafka qu'une fois le fichier vraiment sauvegardé sur S3. C'est ce qui permet le test de crash décrit dans `kafka-connect/RESILIENCE.md` : un crash ne peut ni perdre de données ni les dupliquer en silence.
- La table Bronze dans Athena utilise la projection de partitions au lieu d'un crawler Glue. C'est moins cher et il n'y a rien à faire tourner sur un cron.
- Les doublons (Kafka peut renvoyer un message deux fois) sont supprimés dans dbt en gardant la copie la plus récente de chaque `event_id`, pas dans le producteur.
- IAM est séparé en deux utilisateurs : un qui peut seulement écrire dans le préfixe Bronze de S3 (Kafka Connect), un qui peut lire Bronze et lire/écrire Silver et Gold (dbt). Aucun des deux ne peut toucher les données de l'autre.

### Tests

- `tests/`: tests unitaires pytest pour la validation des trades et le client Kafka
- `dbt/tests/`: tests de schéma (not null, unique, valeurs acceptées) et deux tests personnalisés qui vérifient que le prix et la taille sont toujours positifs
- `kafka-connect/RESILIENCE.md`: un vrai test de crash et de récupération sur le connecteur S3 sink
