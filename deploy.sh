#!/bin/bash
# ==============================================================================
# Script de Deploy Completo - Olist Analytics Pipeline
# ==============================================================================
# Este script executa o deploy completo da infraestrutura via Serverless:
# - Lambda Function
# - S3 Buckets (Data Lake, Glue Scripts, Athena Results)
# - Glue Job, Database e Crawler
# ==============================================================================

set -e  # Sair em caso de erro

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Stage (pode ser passado como argumento ou usa 'dev' como padrão)
STAGE=${1:-dev}

# Obter Account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "<account-id>")

echo -e "${BLUE}=============================================${NC}"
echo -e "${BLUE}   Olist Analytics Pipeline - Deploy${NC}"
echo -e "${BLUE}   Stage: ${YELLOW}${STAGE}${NC}"
echo -e "${BLUE}   Account: ${YELLOW}${ACCOUNT_ID}${NC}"
echo -e "${BLUE}=============================================${NC}"
echo ""

# ==============================================================================
# DEPLOY
# ==============================================================================
echo -e "${GREEN}[1/3]${NC} Instalando dependências npm..."
npm install

echo ""
echo -e "${GREEN}[2/3]${NC} Fazendo deploy do Serverless Framework..."
echo -e "${YELLOW}       (Lambda, S3, Glue Job, Database, Crawler)${NC}"
npx serverless deploy --stage $STAGE

echo ""
echo -e "${GREEN}[3/3]${NC} Configurando dependências Python do Glue..."
GLUE_BUCKET="olist-glue-scripts-${ACCOUNT_ID}-${STAGE}"
GLUE_JOB="olist-etl-${STAGE}"

# Upload do requirements.txt
aws s3 cp glue/requirements.txt "s3://${GLUE_BUCKET}/requirements.txt"
echo -e "${GREEN}✓${NC} requirements.txt enviado para s3://${GLUE_BUCKET}/requirements.txt"

# Atualizar job com argumentos de Python modules
echo -e "${GREEN}✓${NC} Atualizando Glue job com parâmetros de dependências..."
aws glue update-job --job-name "${GLUE_JOB}" --job-update \
  "Command={Name=glueetl,ScriptLocation=s3://${GLUE_BUCKET}/scripts/olist_etl.py,PythonVersion=3},\
DefaultArguments={\
--enable-metrics=true,\
--enable-spark-ui=true,\
--enable-glue-datacatalog=true,\
--enable-continuous-cloudwatch-log=true,\
--job-language=python,\
--python-modules-installer-option=-r,\
--additional-python-modules=s3://${GLUE_BUCKET}/requirements.txt,\
--bucket_name=olist-datalake-${ACCOUNT_ID}-${STAGE},\
--redshift_workgroup=olist-redshift-workgroup,\
--redshift_database=olistdb}"

echo -e "${GREEN}✓${NC} Dependências Python configuradas com sucesso!"

# ==============================================================================
# CONCLUSÃO
# ==============================================================================
echo ""
echo -e "${GREEN}=============================================${NC}"
echo -e "${GREEN}   Deploy concluído com sucesso!${NC}"
echo -e "${GREEN}=============================================${NC}"
echo ""
echo -e "Recursos criados:"
echo -e "  ${BLUE}Lambda:${NC}"
echo -e "    - olist-analytics-pipeline-${STAGE}-ingest"
echo ""
echo -e "  ${BLUE}S3 Buckets:${NC}"
echo -e "    - olist-datalake-${ACCOUNT_ID}-${STAGE}"
echo -e "    - olist-glue-scripts-${ACCOUNT_ID}-${STAGE}"
echo -e "    - olist-athena-results-${ACCOUNT_ID}-${STAGE}"
echo ""
echo -e "  ${BLUE}Glue:${NC}"
echo -e "    - Job: olist-etl-${STAGE}"
echo -e "    - Database: olist_datalake_${STAGE}"
echo -e "    - Crawler: olist-processed-crawler-${STAGE}"
echo ""
echo -e "${YELLOW}Próximos passos:${NC}"
echo -e "  1. Execute a Lambda para ingerir dados:"
echo -e "     aws lambda invoke --function-name olist-analytics-pipeline-${STAGE}-ingest response.json"
echo -e "  2. Execute o Glue Job para processar:"
echo -e "     aws glue start-job-run --job-name olist-etl-${STAGE}"
echo -e "  3. Execute o Crawler para catalogar:"
echo -e "     aws glue start-crawler --name olist-processed-crawler-${STAGE}"
echo -e "  4. Consulte os dados no Athena!"
