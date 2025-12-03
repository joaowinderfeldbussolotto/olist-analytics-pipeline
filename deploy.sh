#!/bin/bash
# ==============================================================================
# Script de Deploy Completo - Olist Analytics Pipeline
# ==============================================================================
# Este script executa o deploy completo da infraestrutura:
# 1. Instala dependências npm (Serverless Framework)
# 2. Faz deploy do Serverless (Lambda, S3, Glue Job)
# 3. Instala dependências Python do CDK
# 4. Faz deploy do CDK (Glue Database e Crawler)
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

echo -e "${BLUE}=============================================${NC}"
echo -e "${BLUE}   Olist Analytics Pipeline - Deploy${NC}"
echo -e "${BLUE}   Stage: ${YELLOW}${STAGE}${NC}"
echo -e "${BLUE}=============================================${NC}"
echo ""

# ==============================================================================
# FASE 1: Serverless Framework
# ==============================================================================
echo -e "${GREEN}[1/4]${NC} Instalando dependências npm..."
npm install

echo ""
echo -e "${GREEN}[2/4]${NC} Fazendo deploy do Serverless Framework..."
echo -e "${YELLOW}       (Lambda, S3 Buckets, Glue Job)${NC}"
npx serverless deploy --stage $STAGE

# ==============================================================================
# FASE 2: AWS CDK
# ==============================================================================
echo ""
echo -e "${GREEN}[3/4]${NC} Configurando ambiente Python para CDK..."

# Criar venv se não existir
if [ ! -d "cdk/.venv" ]; then
    echo "       Criando virtual environment..."
    python3 -m venv cdk/.venv
fi

# Ativar venv e instalar dependências
source cdk/.venv/bin/activate
pip install -q -r cdk/requirements.txt

echo ""
echo -e "${GREEN}[4/4]${NC} Fazendo deploy do CDK..."
echo -e "${YELLOW}       (Glue Database, Glue Crawler)${NC}"

cd cdk

# Bootstrap CDK (necessário na primeira execução)
echo "       Verificando bootstrap do CDK..."
cdk bootstrap --context stage=$STAGE 2>/dev/null || true

# Deploy
cdk deploy --context stage=$STAGE --require-approval never

cd ..

# Desativar venv
deactivate

# ==============================================================================
# CONCLUSÃO
# ==============================================================================

# Obter Account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "<account-id>")

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
echo -e "  ${BLUE}CDK:${NC}"
echo -e "    - Glue Database: olist_datalake_${STAGE}"
echo -e "    - Glue Crawler: olist-processed-crawler-${STAGE}"
echo ""
echo -e "${YELLOW}Próximos passos:${NC}"
echo -e "  1. Execute a Lambda para ingerir dados: aws lambda invoke --function-name olist-analytics-pipeline-${STAGE}-ingest response.json"
echo -e "  2. Execute o Glue Job para processar: aws glue start-job-run --job-name olist-etl-${STAGE}"
echo -e "  3. Execute o Crawler para catalogar: aws glue start-crawler --name olist-processed-crawler-${STAGE}"
echo -e "  4. Consulte os dados no Athena!"
