import dlt

catalog = "claim_insurance"
gold_schema = "3_gold"
silver_schema = "2_silver"

# --- MERGED CLAIM-POLICY-CUSTOMER TABLE ---
@dlt.table(name=f"{catalog}.{gold_schema}.customer_claim_policy",
           comment = "Curated claim joined with policy records",
           table_properties={"quality": "gold"})
@dlt.expect_all({"policy_id_is_null" : "policy_id IS NOT NULL",
                 "customer_id_is_null" : "customer_id IS NOT NULL",
                 "claim_id_is_null" : "claim_id IS NOT NULL"})
def customer_claim_policy():
    
    # READ CLEANED DATA (FROM SILVER LAYER)
    claim = dlt.readStream(f"{catalog}.{silver_schema}.claims")
    policy = dlt.read(f"{catalog}.{silver_schema}.policies")
    customer = dlt.read(f"{catalog}.{silver_schema}.customers") 

    # MERGE ALL CLEANED TABLES
    claim_policy = claim.join(policy, "policy_id", how = "left")
    merged_data  = claim_policy.join(other = customer, on = "customer_id", how = "left")

    return merged_data

# -- AGGREGATED TELEMATICS --
@dlt.table(name=f"{catalog}.{gold_schema}.aggregated_telematics",
           comment="Average telematics",
           table_properties={"quality": "gold"}
           )
def telematics():

    # READ TELEMATICS DATA FROM SILVER LAYER
    df = dlt.read(f"{catalog}.{silver_schema}.telematics")

    # AGGREGATED TELEMATICS
    df_agg = df.groupBy("chassis_id").agg({"speed": "avg", "latitude": "avg", "longitude": "avg"})
    df_agg = df_agg.withColumnRenamed("avg(speed)", "avg_speed") \
                   .withColumnRenamed("avg(latitude)", "avg_latitude") \
                   .withColumnRenamed("avg(longitude)", "avg_longitude")

    return df_agg










