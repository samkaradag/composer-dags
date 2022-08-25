bq load \
    --autodetect \
    --project_id=elt-demo-354311 \
    --source_format=CSV \
    --skip_leading_rows=1 \
    staging.skt_mart \
    gs://elt-demo-source/skt_mart.csv

bq load \
    --autodetect \
    --project_id=elt-demo-354311 \
    --source_format=CSV \
    --skip_leading_rows=1 \
    staging.skt_fall \
    gs://elt-demo-source/skt_fall.csv

bq load \
    --autodetect \
    --project_id=elt-demo-354311 \
    --source_format=CSV \
    --skip_leading_rows=1 \
    staging.skt_res_zahl \
    gs://elt-demo-source/skt_res_zahl.csv



bq load \
    --project-id=elt-demo-354311
    --source_format=CSV \
    --skip_leading_rows=1 \
    staging.skt_mart \
    gs://elt-demo-source/skt_mart.csv \
    customer:numeric,age:numeric,income:numeric,purchased_product:string 


