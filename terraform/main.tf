terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "app_rg" {
  name     = "tablextract-rg"
  location = "Germany West Central"
}

resource "azurerm_resource_group" "func_rg" {
  name     = "tablextract-function-group"
  location = "Germany West Central"
}

