terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
  }
}

provider "azurerm" {
  features {}
}

resource "random_string" "random" {
  length  = 8
  special = false
  upper   = false
}

locals {
  suffix   = random_string.random.result
  location = "Germany West Central"
}

resource "azurerm_resource_group" "app_rg" {
  name     = "tablextract-rg-${local.suffix}"
  location = local.location
}

resource "azurerm_resource_group" "func_rg" {
  name     = "tablextract-function-group-${local.suffix}"
  location = local.location
}

resource "azurerm_storage_account" "documents" {
  name                     = "txdocs${local.suffix}"
  resource_group_name      = azurerm_resource_group.func_rg.name
  location                 = azurerm_resource_group.func_rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_storage_container" "documents" {
  name                  = "documents"
  storage_account_name  = azurerm_storage_account.documents.name
  container_access_type = "private"
}

resource "azurerm_application_insights" "func_ai" {
  name                = "tablextract-ai-${local.suffix}"
  location            = azurerm_resource_group.func_rg.location
  resource_group_name = azurerm_resource_group.func_rg.name
  application_type    = "web"
}

resource "azurerm_service_plan" "func_plan" {
  name                = "tx-func-plan-${local.suffix}"
  resource_group_name = azurerm_resource_group.func_rg.name
  location            = azurerm_resource_group.func_rg.location
  os_type             = "Linux"
  sku_name            = "Y1"
}

resource "azurerm_linux_function_app" "tablextract_func" {
  name                       = "tx-func-${local.suffix}"
  resource_group_name        = azurerm_resource_group.func_rg.name
  location                   = azurerm_resource_group.func_rg.location
  service_plan_id            = azurerm_service_plan.func_plan.id
  storage_account_name       = azurerm_storage_account.documents.name
  storage_account_access_key = azurerm_storage_account.documents.primary_access_key
  site_config {
    application_insights_key = azurerm_application_insights.func_ai.instrumentation_key
  }
  app_settings = {
    AzureWebJobsStorage                = azurerm_storage_account.documents.primary_connection_string
    APPINSIGHTS_INSTRUMENTATIONKEY     = azurerm_application_insights.func_ai.instrumentation_key
    FUNCTIONS_WORKER_RUNTIME           = "python"
  }
}

resource "azurerm_service_plan" "web_plan" {
  name                = "tx-web-plan-${local.suffix}"
  resource_group_name = azurerm_resource_group.app_rg.name
  location            = azurerm_resource_group.app_rg.location
  os_type             = "Linux"
  sku_name            = "B1"
}

resource "azurerm_linux_web_app" "ui" {
  name                = "tablextract-ui-${local.suffix}"
  resource_group_name = azurerm_resource_group.app_rg.name
  location            = azurerm_resource_group.app_rg.location
  service_plan_id     = azurerm_service_plan.web_plan.id
  site_config {
    always_on = true
  }
}

resource "azurerm_cognitive_account" "doc_intel" {
  name                = "tablextract-di-${local.suffix}"
  location            = azurerm_resource_group.app_rg.location
  resource_group_name = azurerm_resource_group.app_rg.name
  kind                = "FormRecognizer"
  sku_name            = "S0"
}

