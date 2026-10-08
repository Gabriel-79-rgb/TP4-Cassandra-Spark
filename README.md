# TP4 : de Cassandra à Spark (films OMDb)

Lecture de ma table Cassandra `movies_cluster.movies` depuis un notebook PySpark, puis analyse métier avec des DataFrames Spark. Observation des partitions, de la Lazy Evaluation, des jobs / stages / tasks et du shuffle dans le plan d'exécution et dans la Spark UI.

## Contenu du dépôt

```
TP4-Cassandra-Spark/
├── README.md
├── notebook/
│   └── tp4_cassandra_spark.ipynb   # notebook exécuté, sorties visibles
└── screenshots/                    # captures citées ci-dessous
```

## Environnement

- Cluster Cassandra 4.1 à 3 nœuds (`cass1`, `cass2`, `cass3`), RF = 3, réseau Docker `cass-net`.
- Conteneur `spark-tp1` (image `jupyter/pyspark-notebook`) rattaché à `cass-net`, Jupyter sur le port 8888, Spark UI sur le port 4040.
- Spark 4.2.0, `master = local[4]` (4 cœurs, parallélisme par défaut = 4), `spark.sql.shuffle.partitions = 4`.
- Connexion : `Cluster(["cass1"])` avec `cassandra-driver`. Les lignes lues sont converties en tuples, puis en DataFrame avec un schéma explicite (`spark.createDataFrame`). Les warnings du driver à la connexion (protocole, load balancing policy) sont informatifs et sans conséquence.

Capture du réseau : `screenshots/00_reseau_cass-net.png`.

## Données utilisées

Table `movies_cluster.movies` : 100 films OMDb, clé de partition `imdb_id`.

| Colonne | Type |
|---|---|
| imdb_id | text (partition key) |
| title | text |
| year | int |
| runtime | text (ex. « 116 min ») |
| genres, directors, countries | list&lt;text&gt; |
| imdb_rating | double |
| imdb_votes | int |

Colonnes importantes pour l'analyse : `year`, `imdb_rating`, `imdb_votes`, `runtime`, `genres`.

Captures : `01_connexion_cassandra.png`, `02a_dataframe_tableau.png`, `02b_dataframe_schema.png` (100 lignes).

## Question métier

> Comment évolue la note moyenne des films selon la décennie, et ces moyennes reposent-elles sur assez de films pour être fiables ?

## Traitements réalisés

**Sélection de colonnes** : `title, year, runtime, imdb_rating, imdb_votes` (`03a_selection.png`).

**Deux filtres** (`03b_filtres.png`) :
- films très bien notés : `imdb_rating >= 8` ;
- films très populaires : `imdb_votes > 500 000`.

**Colonnes calculées** (`04_colonnes_calculees.png`) :
- `decade = floor(year / 10) * 10` ;
- `runtime_min` : durée numérique extraite de `runtime` avec `regexp_extract` ;
- `nb_genres = size(genres)`.

**Agrégation** : `groupBy("decade")` avec nombre de films, note moyenne, note maximale et nombre moyen de votes (`05a_actions.png`, `05b_agregation.png`).

| décennie | nb_films | note_moyenne | note_max | votes_moyens |
|---|---|---|---|---|
| 1930 | 1 | 8.1 | 8.1 | 120 764 |
| 1950 | 1 | 7.9 | 7.9 | 109 764 |
| 1960 | 5 | 7.56 | 8.3 | 198 017 |
| 1970 | 3 | 7.27 | 8.6 | 596 206 |
| 1980 | 6 | 7.73 | 8.7 | 576 134 |
| 1990 | 8 | 6.5 | 7.8 | 299 379 |
| 2000 | 21 | 7.0 | 8.2 | 374 011 |
| 2010 | 46 | 7.02 | 8.4 | 289 390 |
| 2020 | 9 | 6.82 | 7.8 | 292 396 |

Total : 100 films, aucune note ni année manquante.

**Chaîne complète** (`09_chaine_complete.png`) : Cassandra → DataFrame → sélection → filtre (`imdb_rating` non nul et `imdb_votes > 10 000`) → colonne `decade` → `groupBy` → `orderBy` → `show()`. Le résultat est identique au tableau ci-dessus (nb_films et note_moyenne) : le filtre sur les votes n'écarte aucun film de cet échantillon.

## Réponse à la question métier

- Les notes moyennes les plus hautes sont celles de 1930 (8.1) et 1950 (7.9), mais elles reposent sur **un seul film** chacune : elles ne sont pas fiables.
- Les décennies qui pèsent vraiment sont 2000 (21 films, 7.0) et 2010 (46 films, 7.02) : leurs moyennes sont très proches, la note moyenne est stable autour de 7.
- 1990 a la moyenne la plus basse (6.5) sur 8 films, et 2020 est à 6.82 sur 9 films.
- Sur cet échantillon, on ne peut donc pas conclure à une vraie évolution de la qualité : les décennies anciennes sont trop peu représentées, ce qui rend la comparaison fragile. La colonne `nb_films` est indispensable pour interpréter la moyenne. L'échantillon de 100 films n'est pas représentatif de l'ensemble des films.

## Partitions

Résultats (`06_partitions.png`) :

| DataFrame | Partitions | Lignes par partition |
|---|---|---|
| `df` (issu de Cassandra) | 4 | [25, 25, 25, 25] |
| `df.repartition(8)` | 8 | [13, 12, 14, 12, 12, 12, 13, 12] |

Le DataFrame est créé depuis une liste Python : Spark la découpe en 4 partitions de 25 lignes, une par cœur de `local[4]`. `repartition(8)` redistribue les lignes (avec un shuffle) en 8 partitions presque égales.

## Lazy Evaluation

Les transformations `filter`, `withColumn` et `groupBy` (`etape1`, `etape2`, `etape3`) ne déclenchent aucun calcul : Spark construit seulement un plan (`07_lazy_evaluation.png`). Le calcul n'a lieu qu'avec l'action `show()` (`07_lazy_evaluation.png`).

Intérêt : Spark voit tout le plan avant de l'exécuter. Il peut l'optimiser avec Catalyst (par exemple ne garder que les colonnes utiles et pousser le filtre, ce que montre le plan optimisé) et éviter des calculs inutiles.

## Plan d'exécution et shuffle

`df_decade.explain()` (`08a_plan_physique.png`) :

```
Sort [decade ASC]
+- Exchange rangepartitioning(decade, 4)          <- shuffle pour le orderBy
   +- HashAggregate(keys=[decade], count, avg, max, avg)   <- agrégation finale
      +- Exchange hashpartitioning(decade, 4)     <- shuffle pour le groupBy
         +- HashAggregate(... partial_count, partial_avg ...)  <- agrégation partielle
            +- Project (decade = floor(year / 10) * 10)
               +- Filter (imdb_rating et year non nuls)
                  +- Scan ExistingRDD
```

- `Exchange hashpartitioning(decade, 4)` est le **shuffle du `groupBy`** : toutes les lignes d'une même décennie sont regroupées dans la même partition.
- `Exchange rangepartitioning(decade, 4)` est le **shuffle du `orderBy`**, qui répartit les données par plages de décennies.
- Le `HashAggregate` partiel fait une pré-agrégation dans chaque partition avant le shuffle, ce qui réduit les données échangées. Le `HashAggregate` final termine le calcul après.
- `explain(True)` montre aussi les plans logique, analysé et optimisé : dans le plan optimisé, Catalyst a fusionné les projections et ne garde que `year`, `imdb_rating` et `imdb_votes` (`08b_plans_logiques.png`, `08c_plans_optimise_physique.png`).

## Spark UI

Observations dans la Spark UI (`http://localhost:4040`) :

- **Jobs** (`10_ui_jobs.png`) : une action (`show`, `count`, `collect`) crée au moins un job. La liste contient beaucoup de jobs car les cellules ont été relancées plusieurs fois. Une action avec `orderBy` en crée parfois plusieurs.
- **Job 54** (`11a_ui_job_resume.png`, `11b_ui_job_dag.png`, `11c_ui_job_stage_execute.png`) : durée 48 ms, 1 stage exécuté (stage 71) et 1 stage sauté (stage 70). Le DAG montre le stage 70 (lecture, filtre, décennie, agrégation partielle) qui se termine par un `Exchange`, puis le stage 71 (`AQEShuffleRead`, agrégation finale, tri et affichage). Le stage 70 est « skipped » car ses fichiers de shuffle ont déjà été produits par l'exécution précédente et sont réutilisés.
- **Stage 68** (job 52, `12_ui_shuffle.png`) : 1 task de 12 ms, `Shuffle Read Size / Records : 2.2 KiB / 38`. Ce stage lit les agrégats partiels écrits par le stage précédent.
- **Requête SQL 30** (`13a_ui_sql_plan.png`, `13b_ui_sql_plan.png`) : plan SQL avec les nœuds `Exchange` et leurs métriques.

Pourquoi il y a un shuffle : un `groupBy` doit réunir au même endroit toutes les lignes ayant la même clé (`decade`). Les données sont donc écrites par les tasks du premier stage, puis relues par les tasks du stage suivant. Le shuffle coûte cher (écriture disque, transfert réseau), c'est pourquoi Spark pré-agrège avant de l'effectuer.

AQE (Adaptive Query Execution) ajuste le nombre de partitions après le shuffle, d'où un seul task dans le stage de lecture.

## Justification

- **Pourquoi ces données** : les films OMDb ont une année et une note, ce qui permet de suivre une évolution dans le temps. La table est celle que j'ai alimentée dans les TP précédents.
- **Pourquoi ces transformations** : le filtre sur les votes écarte les films dont la note est peu fiable, et la décennie regroupe les années pour comparer des périodes.
- **Pourquoi cette agrégation** : `avg` donne la note moyenne et `count` indique si la moyenne repose sur assez de films. Une moyenne sur 1 film n'est pas fiable.
- **Ce qui déclenche l'exécution** : uniquement les actions (`show`, `count`, `collect`). Tout ce qui précède construit le plan.
- **Shuffle** : visible dans le plan (`Exchange`) et dans la Spark UI (frontière entre stages, Shuffle Read / Write).