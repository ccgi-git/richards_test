import os
import json
import sys
import re
import pdb
import pandas as pd
import ccgi_utilities
from snowflake.snowpark.functions import udf, col, lit, translate, is_null, iff, sum, max, call_udf


class File_Import():
    def __init__(self):
        self.credentials = ccgi_utilities.CCGI_UTILITIES()
        self.snow_cred = self.credentials.get_secret("snowflake_credentials", self.credentials.get_aws_session())
        self.ccgi_cred = self.credentials.get_aws_session()
        self.spark_session = self.credentials.snowflake(self.snow_cred)
    def runner(self):
        choice = os.environ.get('FILE_TYPE')
        print(f'Choices selected:{choice}')


        print('full query running')
        data = self.import_query(flag = choice.lower())


        directory = '/snow_parquet/'

        self.map_upload_to_s3(directory,data)
        print('-'*20,'Dataframe Generated','-'*20)

        return

    def map_upload_to_s3(self, directory, data):
        # Upload local files to S3
        # remember, there are integers appended to the end of some of the districts, we want to remove these when we upload the files
        print("*----------------Write to dataframe----------------*")
        print(data)
        dir = os.path.join(directory, 'files_to_process.csv')
        data.to_csv(dir, index = False)

        print("*----------------Uploading to S3 Bucket----------------*")

        s3_client_ccgi = self.ccgi_cred.client('s3')
        s3_client_ccgi.upload_file(dir, Bucket='richards-bucketv2',
                                   Key="mapper/files_to_process.csv")

        if data.empty:
            self.send_task('end process')

        return
    def send_task(self, msg):
        task_token = self.ccgi_cred.client('stepfunctions')
        token = os.environ.get('TASK_TOKEN')
        output = json.dumps(msg)
        response = task_token.send_task_success(taskToken=token,
                                                output=output)

        return response

    def partition(self, data):
        students = data[0]
        courses = data[1]
        choice = os.environ.get('FILE_TYPE')
        msg = []

        # Specify the MAX partition size
        partition_size = 10

        # Calculate the number of containers needed
        stu_part = len(students) // partition_size + (len(students) % partition_size != 0)
        course_part = len(courses) // partition_size + (len(courses) % partition_size != 0)

        """
        Partition the DataFrames into chunks as a list of DFs as string types.
        They need to be transformed from DFs otherwise they won't be JSON serializable

        Once the strings are sent to their respective containers, you will need to convert
        them back into DFs.

        partitions = [df.iloc[i * partition_size: (i + 1) * partition_size].to_string(index = False) for i in range(num_partitions)]
        string_d = StringIO(partitions[0])
        df_t = pd.read_csv(string_d, index_col = False)
        """
        if ('all' in choice) or ('students' in choice):
            for i in range(stu_part):
                stu_dfs = {f'students_{i}': students.iloc[i * partition_size: (i + 1) * partition_size].to_string(index=False)}
                msg.append(stu_dfs)
        if ('all' in choice) or ('course_grades' in choice):
            for i in range(course_part):
                course_dfs = {f'course_grades_{i}':courses.iloc[i * partition_size: (i + 1) * partition_size].to_string(index=False)}
                msg.append(course_dfs)

        return msg
    def import_query(self, flag):
        self.spark_session.sql('use role DATAENGINEER').collect()
        self.spark_session.sql('use warehouse ELT').collect()

        reader = {
            'archive':['DISTRICT_TRANSCRIPT_FILES.PROD2_ARCHIVE_STUDENTS',
                       'DISTRICT_TRANSCRIPT_FILES.PROD2_ARCHIVE_COURSE_GRADES',
                       'DISTRICT_TRANSCRIPT_FILES.CCGI_PROD_TRANSCRIPT_ARCHIVE',
                       'V_CCGI_PROD_TRANSCRIPT_ARCHIVE'],
            'review':['DISTRICT_TRANSCRIPT_FILES.REVIEW_IMPORT_STUDENTS',
                      'DISTRICT_TRANSCRIPT_FILES.REVIEW_IMPORT_COURSE_GRADES',
                      'DISTRICT_TRANSCRIPT_FILES.CCGI_REVIEW_TRANSCRIPT_IMPORTER',
                      'V_CCGI_REVIEW_TRANSCRIPT_IMPORTER'],
            'import':['DISTRICT_TRANSCRIPT_FILES.PROD_IMPORT_STUDENTS',
                      'DISTRICT_TRANSCRIPT_FILES.PROD_IMPORT_COURSE_GRADES',
                      'DISTRICT_TRANSCRIPT_FILES.CCGI_PROD_TRANSCRIPT_IMPORT',
                      'V_CCGI_PROD_TRANSCRIPT_IMPORT']
        }

        # Create a filter from previous QA tables to determine if we should load files
        schema = reader.get(flag)[0]
        self.spark_session.sql(f'use schema {schema}').collect()
        scan_students = self.spark_session.sql("""select "key" from QA_STUDENTS_BRONZE""").toPandas()

        schema = reader.get(flag)[1]
        self.spark_session.sql(f'use schema {schema}').collect()
        scan_grades = self.spark_session.sql("""select "key" from QA_COURSE_GRADES_BRONZE""").toPandas()

        full_list = pd.concat([scan_students, scan_grades]).reset_index(drop=True)
        full_list = full_list['key'].tolist()
        print(full_list)

        schema = reader.get(flag)[2]
        self.spark_session.sql(f'use schema {schema}').collect()
        view = reader.get(flag)[3]

        print(f'Originating query: {view}')
        print("*----------------Importing flat files----------------*")

        tbl = self.spark_session.sql(f"""

                /*
                This CTE is responsible for IDing legacy_transcripts. If the latest row is not a transcript, it means it is a student/course file without a 
                corresponding legacy_transcript and should not be loaded. If it is a transcript file, it is valid. From there, it appends all the legacy_transcript dates onto 
                student/course keys so we have the corresponding valid transcript file associated.
                */
                
                with COMPARE as (
                select 
                    r.FILE_TYPE as LEGACY_TRANSCRIPT,
                    r.LAST_MODIFIED as LEGACY_DATE,
                    c.KEY as KEY,
                    c.DISTRICT as DISTRICT, 
                    c.FILE_TYPE as FILE_TYPE, 
                    c.FILE_NAME,
                    c.LAST_MODIFIED as LAST_MODIFIED,
                    c.SIZE as SIZE,
                    iff(r.FILE_TYPE='legacy_transcript', 'valid', 'invalid') as IS_TRANSCRIPT
                from {view} r
                full join {view} c on c.DISTRICT=r.DISTRICT
                where ((c.FILE_TYPE = 'student' or c.FILE_TYPE = 'course_grade') 
                    and (c.LAST_MODIFIED < r.LAST_MODIFIED)
                    and datediff(month, c.LAST_MODIFIED, r.LAST_MODIFIED) <=6)
                having IS_TRANSCRIPT = 'valid'
                order by 4 asc),

                FILE_MAX_DATE as (
                select 
                    DISTRICT,
                    FILE_NAME,
                    KEY,
                    FILE_TYPE,
                    LEGACY_DATE,
                    LAST_MODIFIED,
                    SIZE,
                max(LAST_MODIFIED) over(partition by DISTRICT, FILE_TYPE order by LAST_MODIFIED desc, SIZE desc) FINAL_MAX
                ,row_number() over(partition by DISTRICT, FILE_TYPE order by LAST_MODIFIED desc, SIZE desc) as POS
                from COMPARE
                where FILE_TYPE in ('course_grade', 'student')
                group by 1,2,3,4,5,6,7
                )

                select 
                    DISTRICT,
                    FILE_NAME,
                    FILE_TYPE,
                    LAST_MODIFIED,
                    KEY
                from FILE_MAX_DATE
                where FINAL_MAX = LAST_MODIFIED and POS = 1 and DISTRICT not in ('ccgiadmin','ccgiunifiedadmin')
                order by 1 asc
                """)

        sql = tbl.select('*').where((col('FILE_TYPE') == 'course_grade') | (col('FILE_TYPE') == 'student')).toPandas()
        sql['bucket_type'] =[flag]*len(sql)
        sql = sql[~sql['KEY'].isin(full_list)]

        print("*----------------Flat files identified----------------*")
        if sql.empty:
            print("No files to be processed")

        print(len(sql))

        return sql
    def updated_query(self):
        print("*----------------Importing flat files----------------*")
        self.spark_session.sql('use role DATAENGINEER').collect()
        self.spark_session.sql('use schema DISTRICT_TRANSCRIPT_FILES.CCGI_PROD_TRANSCRIPT_ARCHIVE').collect()
        self.spark_session.sql("""create or replace temporary table BRONZE_CCGI_PROD_TRANSCRIPT_ARCHIVE as
            select * from V_CCGI_PROD_TRANSCRIPT_ARCHIVE""").collect()

        tbl = self.spark_session.sql("""
        /*
        Finds the latest updates. Note, that there will be some instances in which there may be 2 course/students at the same time
        but one is WIP and the other is final. Need to grab the largest file size
        */
        with CHANGES as(
        select B.DISTRICT, B.FILE_TYPE, B.FILE_NAME, B.LAST_MODIFIED, B.KEY, B.SIZE
            from BRONZE_CCGI_PROD_TRANSCRIPT_ARCHIVE B
            where B.FILE_TYPE = 'course_grade' or B.FILE_TYPE = 'student' or B.FILE_TYPE = 'legacy_transcript'


        except

        select S1.DISTRICT, S1.FILE_TYPE, S1.FILE_NAME, S1.LAST_MODIFIED, S1.KEY, S1.SIZE
            from SILVER_CCGI_PROD_TRANSCRIPT_ARCHIVE S1
            where S1.FILE_TYPE = 'course_grade' or S1.FILE_TYPE = 'student' or S1.FILE_TYPE = 'legacy_transcript'
            order by 1, 2 asc
        ),

        --gives a row number partitioned by district and ordered by the oldest date
        RANKS as (
        select 
            *,
            row_number() over(partition by DISTRICT order by LAST_MODIFIED, SIZE desc) as RANK_ROW
        from CHANGES
        order by 1 asc),

        /*
        This CTE is responsible for IDing legacy_transcripts. If the latest row is not a transcript, it means it is a student/course file without a 
        corresponding legacy_transcript and should not be loaded. If it is a transcript file, it is valid. From there, it appends all the legacy_transcript dates onto 
        student/course keys so we have the corresponding valid transcript file associated.
        */
        COMPARE as (
        select 
            r.FILE_TYPE as LEGACY_TRANSCRIPT,
            r.LAST_MODIFIED as LEGACY_DATE,
            c.KEY as KEY,
            c.DISTRICT as DISTRICT, 
            c.FILE_TYPE as FILE_TYPE, 
            c.FILE_NAME,
            c.LAST_MODIFIED as LAST_MODIFIED,
            c.SIZE as SIZE,
            iff(max(r.RANK_ROW) and r.FILE_TYPE!='legacy_transcript', 'invalid', 'valid') as IS_TRANSCRIPT
        from RANKS r
        full join CHANGES c on c.DISTRICT=r.DISTRICT
        where c.FILE_TYPE = 'student' or c.FILE_TYPE = 'course_grade' and c.LAST_MODIFIED < r.LAST_MODIFIED
        group by 1,2,3,4,5,6,7,8
        having IS_TRANSCRIPT = 'valid'
        order by 4 asc),

        --You need to include size due to some WIP files. However, the WIP are almost always smaller than the final versions
        FILE_MAX_DATE as (
        select 
            DISTRICT,
            FILE_NAME,
            KEY,
            FILE_TYPE,
            LEGACY_DATE,
            LAST_MODIFIED,
            SIZE,
        max(LAST_MODIFIED) over(partition by DISTRICT, FILE_TYPE order by LAST_MODIFIED desc, SIZE desc) FINAL_MAX
        ,row_number() over(partition by DISTRICT, FILE_TYPE order by LAST_MODIFIED desc, SIZE desc) as POS
        from COMPARE
        where FILE_TYPE in ('course_grade', 'student')
        group by 1,2,3,4,5,6,7
        )

        select 
            DISTRICT,
            FILE_NAME,
            FILE_TYPE,
            LAST_MODIFIED,
            KEY
        from FILE_MAX_DATE
        where FINAL_MAX = LAST_MODIFIED and POS = 1 
        order by 1 asc
        """)

        sql = tbl.select('*').where((col('FILE_TYPE') == 'course_grade') | (col('FILE_TYPE') == 'student')).toPandas()

        print("*----------------Flat files identified----------------*")

        print(sql)
        return sql

if __name__=="__main__":
    # you should run your class inheritance from bottom to top. I.e this file
    # then flat_file_loading
    print(
    """
    current acceptable first arguments:
    review
    archive
    import
    """)

    #File_Import().runner()
    File_Import().import_query(flag = 'import')


