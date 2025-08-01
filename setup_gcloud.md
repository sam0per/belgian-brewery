# Google Cloud CLI Setup Guide

## Installation Options

### Option 1: macOS with Homebrew (Recommended)

```bash
# Install Homebrew if you don't have it
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Google Cloud CLI
brew install google-cloud-sdk

# Add to your shell profile (if not automatically added)
echo 'source "$(brew --prefix)/share/google-cloud-sdk/path.bash.inc"' >> ~/.zshrc
echo 'source "$(brew --prefix)/share/google-cloud-sdk/completion.bash.inc"' >> ~/.zshrc

# Reload your shell
source ~/.zshrc
```

### Option 2: Direct Download (macOS/Linux)

```bash
# Download and run the installer
curl https://sdk.cloud.google.com | bash

# Restart your shell
exec -l $SHELL

# Or manually add to PATH
echo 'source "$HOME/google-cloud-sdk/path.bash.inc"' >> ~/.zshrc
echo 'source "$HOME/google-cloud-sdk/completion.bash.inc"' >> ~/.zshrc
source ~/.zshrc
```

### Option 3: Manual Download

1. Go to [Google Cloud CLI Installation](https://cloud.google.com/sdk/docs/install)
2. Download the appropriate package for your OS
3. Follow the installation instructions

## Initial Setup

After installation, run these commands:

```bash
# Verify installation
gcloud --version

# Initialize gcloud (this will open a browser for authentication)
gcloud init

# Select or create a Google Cloud Project
# Set your default region/zone if prompted
```

## Authentication for BigQuery

```bash
# Set up application default credentials
gcloud auth application-default login

# Verify your current configuration
gcloud config list

# Set your project (if not set during init)
gcloud config set project YOUR_PROJECT_ID

# Enable BigQuery API (if not already enabled)
gcloud services enable bigquery.googleapis.com
```

## Useful Commands

```bash
# List available projects
gcloud projects list

# Switch projects
gcloud config set project PROJECT_ID

# List BigQuery datasets
bq ls

# Check current authentication
gcloud auth list

# Revoke credentials (if needed)
gcloud auth revoke
```

## Troubleshooting

### Command not found
- Make sure the Google Cloud CLI is in your PATH
- Restart your terminal
- Check installation with `which gcloud`

### Authentication issues
- Run `gcloud auth application-default login` again
- Check if you have the right permissions in the Google Cloud project
- Verify project ID with `gcloud config get-value project`

### Permission errors
- Make sure your account has BigQuery permissions
- Check IAM roles in Google Cloud Console
- You need at least: BigQuery User, BigQuery Data Editor, BigQuery Job User