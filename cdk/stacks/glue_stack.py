"""
Stack CDK para Glue Database e Crawler
"""
from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_iam as iam,
    CfnOutput,
)
from constructs import Construct


class GlueStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, stage: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Nomes dos recursos baseados no stage e account ID para unicidade
        bucket_name = f"olist-datalake-{self.account}-{stage}"
        database_name = f"olist_datalake_{stage}"
        crawler_name = f"olist-processed-crawler-{stage}"

        # Referência à LabRole existente
        lab_role = iam.Role.from_role_arn(
            self, "LabRole",
            role_arn=f"arn:aws:iam::{self.account}:role/LabRole"
        )

        # =====================================================
        # GLUE DATABASE
        # =====================================================
        glue_database = glue.CfnDatabase(
            self, "OlistGlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=database_name,
                description="Database do Data Lake Olist - Camada Processed",
                location_uri=f"s3://{bucket_name}/processed/"
            )
        )

        # =====================================================
        # GLUE CRAWLER
        # =====================================================
        glue_crawler = glue.CfnCrawler(
            self, "OlistProcessedCrawler",
            name=crawler_name,
            role=lab_role.role_arn,
            database_name=database_name,
            description="Crawler para catalogar dados processados do Olist",
            
            # Configuração do target S3
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{bucket_name}/processed/",
                        exclusions=[
                            "**/_temporary/**",
                            "**/_spark_metadata/**",
                            "**/.spark-staging/**"
                        ]
                    )
                ]
            ),
            
            # Configuração do schema
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="LOG"
            ),
            
            # Configuração de recrawl
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior="CRAWL_EVERYTHING"
            ),
            
            # Table prefix
            table_prefix="olist_",
            
            # Tags
            tags={
                "Project": "Olist",
                "Environment": stage
            }
        )
        
        # Garantir que o crawler seja criado após o database
        glue_crawler.add_dependency(glue_database)

        # =====================================================
        # OUTPUTS
        # =====================================================
        CfnOutput(
            self, "GlueDatabaseName",
            value=database_name,
            description="Nome do Glue Database"
        )

        CfnOutput(
            self, "GlueCrawlerName",
            value=crawler_name,
            description="Nome do Glue Crawler"
        )

        CfnOutput(
            self, "CrawlerTargetPath",
            value=f"s3://{bucket_name}/processed/",
            description="Path S3 do Crawler"
        )
