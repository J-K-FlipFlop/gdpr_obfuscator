terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "gdpr-obfuscator-state"
    key    = "state-bucket/terraform.tfstate"
    region = "eu-west-2"
  }
}

provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
      ProjectName  = "gdpr obfuscator"
      Team         = "Alex"
      DeployedFrom = "Terraform"
      Repository   = "gdprobfuscator"
    }
  }
}

data "aws_caller_identity" "current" {

}

data "aws_region" "current" {

}
