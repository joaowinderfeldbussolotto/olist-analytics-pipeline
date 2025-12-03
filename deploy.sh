#!/bin/bash
# ==============================================================================
# Script de Deploy Completo - Olist Analytics Pipeline
# ==============================================================================
# Este script executa o deploy completo da infraestrutura:
# 1. Instala dependências npm (Serverless Framework)
# 2. Faz deploy do Serverless (Lambda, S3, Glue Job)
# 3. Faz deploy do CloudFormation (Glue Database e Crawler)
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
# FASE 1: Serverless Framework
# ==============================================================================
echo -e "${GREEN}[1/3]${NC} Instalando dependências npm..."
npm install

echo ""
echo -e "${GREEN}[2/3]${NC} Fazendo deploy do Serverless Framework..."
echo -e "${YELLOW}       (Lambda, S3 Buckets, Glue Job)${NC}"
npx serverless deploy --stage $STAGE

# ==============================================================================
# FASE 2: CloudFormation (Glue Resources)
# ==============================================================================
echo ""
echo -e "${GREEN}[3/3]${NC} Fazendo deploy do CloudFormation..."
echo -e "${YELLOW}       (Glue Database, Glue Crawler)${NC}"

STACK_NAME="olist-glue-resources-${STAGE}"

aws cloudformation deploy \
    --template-file cloudformation/glue-resources.yml \
    --stack-name $STACK_NAME \
    --parameter-overrides Stage=$STAGE \
    --capabilities CAPABILITY_IAM \
    --no-fail-on-empty-changeset

# ==============================================================================
# CONCLUSÃO
# ==============================================================================
echo ""
echo -e "${GREEN}=============================================${NC}"
echo -e "${GREEN}   Deploy concluído com sucesso!${NC}"
echo -e "${GREEN}=============================================${NC}"
echo ""
echo -e "Recursos criados:"
echo -e "  ${BLUE}Serverless:${NC}"
echo -e "    - Lambda: olist-analytics-pipeline-${STAGE}-ingest"
echo -e "    - S3: olist-datalake-${ACCOUNT_ID}-${STAGE}"
echo -e "    - S3: olist-glue-scripts-${ACCOUNT_ID}-${STAGE}"
echo -e "    - S3: olist-athena-results-${ACCOUNT_ID}-${STAGE}"
echo -e "    - Glue Job: olist-etl-${STAGE}"
echo ""
echo -e "  ${BLUE}CloudFormation:${NC}"
echo -e "    - Glue Database: olist_datalake_${STAGE}"
echo -e "    - Glue Crawler: olist-processed-crawler-${STAGE}"
echo ""
echo -e "${YELLOW}Próximos passos:${NC}"
echo -e "  1. Execute a Lambda para ingerir dados: aws lambda invoke --function-name olist-analytics-pipeline-${STAGE}-ingest response.json"
echo -e "  2. Execute o Glue Job para processar: aws glue start-job-run --job-name olist-etl-${STAGE}"
echo -e "  3. Execute o Crawler para catalogar: aws glue start-crawler --name olist-processed-crawler-${STAGE}"
echo -e "  4. Consulte os dados no Athena!"
