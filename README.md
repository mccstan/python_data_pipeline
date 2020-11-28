# python_data_pipeline
A python data pipeline for data engineering

## I.Python et Data Engineering

### Data pipeline

### Traitement ad-hoc

### Pour aller plus loin

## II. SQL

### DDL
Les requêtes SQL dans la suite ont été écrites pour fonctionner dans le moteur de base de données relationelles MySQL(MySQL v5.7).
Elles sont pensées avec simplicité pour s'adapter à la plus part des moteurs d'exécution avec des adaptations.
Ces requêtes peuvent être testées par ici https://www.db-fiddle.com/ en utilisant le DDL suivant.

```sql
CREATE TABLE product_nomenclature (
    product_id bigint PRIMARY KEY,
    product_type VARCHAR(30),
    product_name VARCHAR(30)
);

CREATE TABLE transactions (
    date date,
    order_id bigint,
    client_id bigint,
    prod_id bigint,
    prod_price DECIMAL(10, 2),
    prod_qty bigint,
    FOREIGN KEY (prod_id) REFERENCES product_nomenclature(product_id)
    ON DELETE SET NULL  ON UPDATE CASCADE
);

INSERT INTO product_nomenclature VALUES(490756, "MEUBLE", "Chaise");
INSERT INTO product_nomenclature VALUES(389728, "DECO", "Boule de Noël");
INSERT INTO product_nomenclature VALUES(549380, "MEUBLE", "Canapé");
INSERT INTO product_nomenclature VALUES(293718, "DECO", "Mug");

INSERT INTO transactions VALUES("20/01/01", 1234, 999, 490756, 50, 1);
INSERT INTO transactions VALUES("20/01/01", 1234, 999, 389728, 3.56, 4);
INSERT INTO transactions VALUES("20/01/01", 3456, 845, 490756, 50, 2);
INSERT INTO transactions VALUES("20/01/01", 3456, 845, 549380, 00, 1);
INSERT INTO transactions VALUES("20/01/01", 3456, 845, 293718, 10, 1);
INSERT INTO transactions VALUES("19/01/03", 3457, 846, 490756, 134, 6);
INSERT INTO transactions VALUES("19/02/08", 3458, 847, 389728, 100, 7);
INSERT INTO transactions VALUES("19/05/03", 3459, 848, 490756, 10, 0);
INSERT INTO transactions VALUES("19/12/31", 3460, 849, 549380, 100, 5);
```

### Le chiffre d’affaires (le montant total des ventes), jour par jour, du 1er janvier 2019 au 31 décembre 2019. Le résultat sera trié sur la date à laquelle la commande a été passée.

```sql
SELECT date, SUM(prod_price) AS ventes FROM transactions
    WHERE date BETWEEN '19/01/01' AND '19/12/31'
    GROUP BY date   
```

Exemple de résultat sur la base de l'échantillon dans la partie DDL

---
| date       | ventes |
| ---------- | ------ |
| 2019-01-01 | 10.00  |
| 2019-01-03 | 134.00 |
| 2019-02-08 | 100.00 |
| 2019-05-03 | 10.00  |
| 2019-12-31 | 100.00 |
---

### Les ventes meuble et déco réaliséesles ventes meuble et déco réalisées par client et sur la période allant du 1er janvier 2020 au 31 décembre 2020.
```sql
SELECT clients.client_id, COALESCE(ventes_meubles, 0) AS ventes_meuble, COALESCE(ventes_deco, 0) AS ventes_deco FROM
    (SELECT distinct client_id FROM transactions) AS clients

    LEFT OUTER JOIN 

    (SELECT client_id, SUM(prod_price) AS ventes_meubles FROM transactions, product_nomenclature
    WHERE prod_id = product_id AND product_type = "MEUBLE" AND date BETWEEN '20/01/01' AND '20/12/31'
    GROUP BY client_id) AS ventes_meubles_table
    ON clients.client_id = ventes_meubles_table.client_id

    LEFT OUTER JOIN

    (SELECT client_id, SUM(prod_price) AS ventes_deco FROM transactions, product_nomenclature
    WHERE prod_id = product_id AND product_type = "DECO" AND date BETWEEN '20/01/01' AND '20/12/31'
    GROUP BY client_id) AS ventes_deco_table
    ON clients.client_id = ventes_deco_table.client_id;
```

Exemple de résultat sur la base de l'échantillon dans la partie DDL

---
| client_id | ventes_meuble | ventes_deco |
| --------- | ------------- | ----------- |
| 990       | 0.00          | 979.12      |
| 999       | 50.00         | 36.56       |
| 845       | 50.00         | 0.00        |
| 846       | 0.00          | 0.00        |
| 847       | 0.00          | 0.00        |
| 848       | 0.00          | 0.00        |
| 849       | 0.00          | 0.00        |
---
