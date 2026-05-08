import dlt
from pyspark.sql import functions
from pyspark.sql.functions import *

# 1. == CONFIGURATION ==
catalog = "claim_insurance"
bronze_schema = "1_bronze"
silver_schema = "2_silver"


# 2. == BUILD SILVER MODEL

# CLEANED CLAIM DATA
@dlt.table(name = f"{catalog}.{silver_schema}.claims", comment = "Cleaned Claim Data",
           table_properties = {"quality" : "silver"})
@dlt.expect_all(expectations = { # NON NULL COLUMNS
                                "claim_id_not_null": "claim_id IS NOT NULL",  # PRIMARY KEY
                                "policy_id_not_null": "policy_id IS NOT NULL",
                                "claim_date_not_null": "claim_date IS NOT NULL",  # DATE
                                "incident_date_not_null": "incident_date IS NOT NULL",
                                "incident_hour_not_null": "incident_hour IS NOT NULL",
                                "total_not_null": "total_claim_amount IS NOT NULL",   # CLAIM AMOUNT
                                "injury_claim_not_null": "injury_claim_amount IS NOT NULL",
                                "property_damage_not_null": "property_claim_amount IS NOT NULL",
                                "vehicle_damage_not_null": "vehicle_claim_amount IS NOT NULL",
                                "collision_type_not_null": "collision_type IS NOT NULL",  # INCIDENT DETAILS
                                "severity_not_null": "severity IS NOT NULL",
                                "witnesses_not_null": "number_of_witnesses IS NOT NULL",
                                "customer_age_not_null": "age IS NOT NULL",    # CUSTOMER DATA
                                "tenure_not_null": "months_as_customer IS NOT NULL",

                                # VALID COLUMNS
                                "valid_policy_id" : "policy_id BETWEEN 100000000 AND 999999999",
                                "valid_age" : "age BETWEEN 18 AND 100",
                                "valid_tenure": "months_as_customer >= 0",
                                "valid_hour" : "incident_hour BETWEEN 0 AND 23",
                                "valid_incident_date" : "incident_date <= claim_date",
                                "valid_witnesses" : "number_of_witnesses >= 0",
                                "valid_vehicle_count": "number_of_vehicles_involved > 0",
                                "valid_license_date" : "license_issue_date <= claim_date",
                                "valid_total" : "total_claim_amount = injury_claim_amount + property_claim_amount + vehicle_claim_amount",
                                "valid_severity_level": "severity IN ('Minor Damage', 'Major Damage', 'Total Loss', 'Trivial Damage')",
                                "valid_collision_type": "collision_type IN ('Side Collision', 'Rear Collision', 'Front Collision', 'Parked Car')",
                                "non_negative_amount" : "injury_claim_amount >= 0 AND property_claim_amount >= 0 AND vehicle_claim_amount >= 0 AND total_claim_amount >= 0",
                                })
def claim_silver():

    # LOAD STREAM DATA
    df = dlt.readStream(f"{catalog}.{bronze_schema}.claims")

    clean_df = df.withColumnRenamed(existing = 'claim_no', new = "claim_id") \
                .withColumnRenamed(existing = "policy_no", new = "policy_id") \
                .withColumnRenamed(existing = "date", new = "incident_date") \
                .withColumnRenamed(existing = "hour", new = "incident_hour") \
                .withColumnRenamed(existing = "injury", new = "injury_claim_amount") \
                .withColumnRenamed(existing = "property", new = "property_claim_amount") \
                .withColumnRenamed(existing = "vehicle", new = "vehicle_claim_amount") \
                .withColumnRenamed(existing = "total", new = "total_claim_amount") \
                .withColumn("claim_date", to_date(col("claim_date"), "yyyy-MM-dd")) \
                .withColumn("incident_date", to_date(col("incident_date"), "yyyy-MM-dd")) \
                .withColumn("license_issue_date", to_date(col("license_issue_date"), "yyyy-MM-dd")) \
                .withColumn("injury_claim_amount", col("injury_claim_amount").cast("double")) \
                .withColumn("property_claim_amount", col("property_claim_amount").cast("double")) \
                .withColumn("vehicle_claim_amount", col("vehicle_claim_amount").cast("double")) \
                .withColumn("total_claim_amount", col("total_claim_amount").cast("double")) \
                .drop("_rescued_data")

    # ADD METADATA COLUMNS
    column_comments = {"claim_no": "ID unik laporan klaim.",
                        "policy_no": "Nomor polis asuransi yang diklaim.",
                        "claim_date": "Tanggal pelaporan klaim.",
                        "months_as_customer": "Masa kepesertaan nasabah dalam bulan.",
                        "injury": "Nilai ganti rugi cedera badan.",
                        "property": "Nilai ganti rugi kerusakan properti.",
                        "vehicle": "Nilai ganti rugi kerusakan kendaraan.",
                        "total": "Total nilai klaim keseluruhan.",
                        "collision_type": "Jenis tabrakan (misal: Samping, Depan).",
                        "number_of_vehicles_involved": "Jumlah kendaraan yang terlibat kecelakaan.",
                        "age": "Usia nasabah/pengemudi saat kejadian.",
                        "insured_relationship": "Hubungan pengemudi dengan tertanggung.",
                        "license_issue_date": "Tanggal terbit SIM pengemudi.",
                        "date": "Tanggal kejadian kecelakaan.",
                        "hour": "Jam kejadian kecelakaan (0-23).",
                        "type": "Kategori kecelakaan.",
                        "severity": "Tingkat keparahan kerusakan insiden.",
                        "number_of_witnesses": "Jumlah saksi di lokasi.",
                        "suspicious_activity": "Indikasi kecurangan atau aktivitas mencurigakan (True/False)."
                        }
    
    # APPLY COMMENT/METADATA TO EACH COLUMNS
    for column_name, comment in column_comments.items():
        if column_name in df.columns:
            df = df.withColumn(column_name, functions.col(column_name).alias(column_name, metadata={"comment": comment}))

    return clean_df

# CLEANED POLICY DATA
@dlt.table(name = f"{catalog}.{silver_schema}.policies", comment = "Cleaned Policy Data",
        table_properties = { "quality" : "silver" })
@dlt.expect_all(expectations = { # NON NULL COLUMNS
                                "policy_id_not_null": "policy_id IS NOT NULL",  # PRIMARY KEY
                                "customer_id_not_null": "customer_id IS NOT NULL",
                                "issue_date_not_null": "issue_date IS NOT NULL",  # DATE
                                "expiry_date_not_null": "expiry_date IS NOT NULL",
                                "effective_date_not_null" : "effective_date IS NOT NULL",
                                "make_not_null": "make IS NOT NULL",
                                "model_not_null": "model IS NOT NULL",
                                "model_year_not_null": "model_year IS NOT NULL",
                                "chassis_not_null": "chassis_number IS NOT NULL",
                                "use_not_null": "use_of_vehicle IS NOT NULL",
                                "product_not_null": "product IS NOT NULL",
                                "premium_not_null": "premium IS NOT NULL",
                                "sum_insured_not_null" : "sum_insured IS NOT NULL",
                                "deductable_not_null" : "deductable IS NOT NULL",

                                # VALID COLUMNS
                                "valid_policy_id" : "policy_id BETWEEN 100000000 AND 999999999",
                                "valid_issue_date" : "issue_date <= expiry_date",
                                "valid_expiry_date" : "expiry_date >= issue_date",
                                "valid_make" : "make IN ('Toyota', 'Honda', 'Mitsubishi', 'Nissan', 'Mazda', 'Suzuki', 'Hyundai', 'Kia', 'Ford', 'Chevrolet', 'BMW', 'Mercedes-Benz', 'Audi', 'Volkswagen', 'Jeep', 'Subaru', 'Dodge', 'Chrysler', 'GMC', 'Buick', 'Lincoln')"
                                })
def policy_silver():

    # LOAD STREAM DATA
    df = dlt.readStream(f"{catalog}.{bronze_schema}.policies")

    # ADD METADATA COLUMNS
    column_comments = {"POLICY_NO": "Nomor unik kontrak polis asuransi.",
                        "CUST_ID": "ID unik nasabah (Customer).",
                        "POLICYTYPE": "Tipe polis (contoh: Comprehensive, TLO).",
                        "POL_ISSUE_DATE": "Tanggal penerbitan polis.",
                        "POL_EFF_DATE": "Tanggal mulai berlaku perlindungan.",
                        "POL_EXPIRY_DATE": "Tanggal berakhirnya masa berlaku polis.",
                        "MAKE": "Merek kendaraan (Toyota, Honda, dll).",
                        "MODEL": "Varian atau tipe model kendaraan.",
                        "MODEL_YEAR": "Tahun pembuatan kendaraan.",
                        "CHASSIS_NO": "Nomor rangka kendaraan (identitas unik fisik).",
                        "USE_OF_VEHICLE": "Tujuan penggunaan (Pribadi/Komersial).",
                        "PRODUCT": "Nama produk asuransi.",
                        "SUM_INSURED": "Nilai total pertanggungan/harga kendaraan.",
                        "PREMIUM": "Nominal premi yang dibayar nasabah.",
                        "DEDUCTABLE": "Biaya risiko sendiri per kejadian klaim.",
                        "_rescued_data": "Data mentah yang gagal divalidasi saat proses ingest."
    }
    
    # APPLY COMMENT/METADATA TO EACH COLUMNS
    for col_name, comment_text in column_comments.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, functions.col(col_name).alias(col_name, metadata={"comment": comment_text}))

    # CLEANED POLICIES
    clean_df = df.withColumnsRenamed(colsMap = {"POLICY_NO": "policy_id",
                                                "CUST_ID": "customer_id",
                                                "POLICYTYPE": "policy_type",
                                                "POL_ISSUE_DATE": "issue_date",
                                                "POL_EFF_DATE": "effective_date",
                                                "POL_EXPIRY_DATE": "expiry_date",
                                                "MAKE": "make",
                                                "MODEL": "model",
                                                "MODEL_YEAR": "model_year",
                                                "CHASSIS_NO": "chassis_number",
                                                "USE_OF_VEHICLE": "use_of_vehicle",
                                                "PRODUCT": "product",
                                                "SUM_INSURED": "sum_insured",
                                                "PREMIUM": "premium",
                                                "DEDUCTABLE": "deductable",
                                                "_rescued_data": "rescued_data"
                                                }) \
                .withColumn("issue_date", to_date(col("issue_date"), "yyyy-MM-dd")) \
                .withColumn("effective_date", to_date(col("effective_date"), "yyyy-MM-dd")) \
                .withColumn("expiry_date", to_date(col("expiry_date"), "yyyy-MM-dd")) \
                .withColumn("sum_insured", col("sum_insured").cast("double")) \
                .withColumn("premium", col("premium").cast("double")) \
                .withColumn("deductable", col("deductable").cast("double")) \
                .drop("_rescued_data")

    return clean_df

# CLEANED CUSTOMERS DATA
@dlt.view(name="customers_data")  # VIEW DATA 
@dlt.expect_all({"customer_id_not_null": "customer_id IS NOT NULL",
                "date_of_birth_not_null": "date_of_birth IS NOT NULL",
                "borough_not_null": "borough IS NOT NULL",
                "neighborhood_not_null": "neighborhood IS NOT NULL",
                "zip_code_not_null": "zip_code IS NOT NULL",
                "name_not_null": "first_name IS NOT NULL",
                "last_name_not_null": "last_name IS NOT NULL"
                })
def customers_data():

    # LOAD BATCH STREAMING DATA
    df = dlt.readStream(f"{catalog}.{bronze_schema}.customers")

    # METADATA COLUMNS
    column_comments = {"customer_id": "ID unik untuk identifikasi nasabah.",
                        "date_of_birth": "Tanggal lahir nasabah (digunakan untuk perhitungan usia).",
                        "borough": "Nama wilayah administratif (Borough) di NYC.",
                        "neighborhood": "Nama kawasan/lingkungan spesifik tempat tinggal.",
                        "zip_code": "Kode pos lokasi nasabah.",
                        "name": "Nama lengkap nasabah (Format: Lastname, Firstname)."
                        }

    # CLEANED DATA
    clean_data = df.withColumn(colName = "customer_id", col = col("customer_id").cast("int")) \
                .withColumn(colName = "date_of_birth", col = to_date(col("date_of_birth"), "dd-MM-yyyy")) \
                .withColumn(colName = "date_of_birth", col = date_format(col("date_of_birth"), "yyyy-MM-dd")) \
                .withColumn(colName = "first_name", col = split(col("name"), ",").getItem(1)) \
                .withColumn(colName = "last_name", col = split(col("name"), ",").getItem(0)) \
                .withColumn(colName = "borough", col = lower(col("borough"))) \
                .withColumn(colName = "neighborhood", col = upper(col("neighborhood"))) \
                .drop("name", "_rescued_data")

    # APPLY COMMENT/METADATA TO EACH COLUMNS
    for col_name, comment_text in column_comments.items():
        if col_name in clean_data.columns:
            clean_data = clean_data.withColumn(col_name, functions.col(col_name).alias(col_name, metadata={"comment": comment_text}))

    return clean_data

# CREATE CUSTOMERS TABLE
dlt.create_streaming_table(name="customers",
                           comment="Cleaned Customer data with Upsert logic",
                           table_properties={"quality": "silver"}
                          )

# OVERWRITE DATA BARU JIKA customer_id SUDAH ADA DI DATA LAMA (MISAL KALAU CUSTOMER 101 UDAH ADA, MAKA DATA BARU UNTUK CUSTOMER 101 AKAN DI OVERWRITE)
dlt.apply_changes(target = f"{catalog}.{silver_schema}.customers",
                source = 'customers_data',
                keys = ["customer_id"],    # KATA KUNCI OVERWRITE
                sequence_by = col("customer_id"),
                stored_as_scd_type = 1)   # OVERWRITE / REPLACE KOLOM LAMA

# TELEMATICS DATA
@dlt.table(name="telematics", comment="Cleaned Telematics data",
       table_properties = {"quality" : "silver"})
@dlt.expect_all(expectations = {"chassis_no_not_null": "chassis_id IS NOT NULL",
                              "latitude_not_null": "latitude IS NOT NULL",
                              "longitude_not_null": "longitude IS NOT NULL",
                              "event_timestamp_not_null": "event_timestamp IS NOT NULL",
                              "speed_not_null": "speed IS NOT NULL"})
def telematics():
    df = dlt.readStream(f"{catalog}.{bronze_schema}.telematics")

    # DEFINE METADATA 
    column_comments = {"chassis_id": "Nomor rangka kendaraan sebagai identitas unik unit.",
                       "latitude": "Koordinat garis lintang posisi kendaraan.",
                       "longitude": "Koordinat garis bujur posisi kendaraan.",
                       "event_timestamp": "Waktu kejadian (tanggal dan jam) yang terekam oleh sensor.",
                       "speed": "Kecepatan kendaraan saat kejadian terekam."}
    
    # APPLY METADATA TO COLUMNS
    for col_name, comment_text in column_comments.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).alias(col_name, metadata={"comment": comment_text}))

    # DROP ALL DUPLICATED
    df = df.dropDuplicates(subset=["chassis_no", "event_timestamp"])

    # DATA CLEANING & TRANSFORMATION
    clean_df = df.withColumnRenamed("chassis_no", "chassis_id") \
                 .withColumn("event_timestamp", to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
                 .withColumn("latitude", col("latitude").cast("double")) \
                 .withColumn("longitude", col("longitude").cast("double")) \
                 .withColumn("speed", col("speed").cast("double")) \
                 .drop("_rescued_data")

    return clean_df

# CLEAN TRAINING IMAGES
@dlt.table(name = "training_images",
           comment="Cleaned accident training images",
           table_properties={"quality": "silver"})
def training_images():

    # LOAD IMAGE DATA FROM BRONZE LAYER
    df = dlt.readStream(f"{catalog}.{bronze_schema}.training_images")
    df = df.withColumn("label", regexp_extract("path", r"/(\d+)-([a-zA-Z]+)(?: \(\d+\))?\.png$", 2)) # ADD LABEL COLUMN

    return df

# CLEAN TEST IMAGES
@dlt.table(name= "test_images",
           comment="Enriched claim images",
           table_properties={"quality": "silver"})
def test_images():

    # LOAD TEST IMAGE FROM BRONZE LAYER
    df = dlt.readStream(f"{catalog}.{bronze_schema}.test_images")
    df = df.withColumn("image_name", regexp_extract(col("path"), r".*/(.*?.jpg)", 1)) # ADD IMAGE NAME COLUMN

    return df











