#!/usr/bin/env python3
"""
CDK App para infraestrutura Glue (Database e Crawler)
"""
import os
import aws_cdk as cdk
from stacks.glue_stack import GlueStack

app = cdk.App()

# Obter stage do contexto ou usar 'dev' como padrão
stage = app.node.try_get_context("stage") or "dev"

GlueStack(
    app, 
    f"OlistGlueStack-{stage}",
    stage=stage,
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region=os.environ.get("CDK_DEFAULT_REGION", "us-east-1")
    )
)

app.synth()
