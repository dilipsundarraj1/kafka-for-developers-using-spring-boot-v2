# Docker Installation Guide

## What is Docker and Why Do You Need It?

Docker is a platform that lets you run applications inside lightweight, isolated containers. Instead of installing tools directly on your machine and dealing with version conflicts or OS differences, you define what you need and Docker runs it consistently — on any machine.

### Key Benefits

- **No more "works on my machine"** — containers run the same way everywhere
- **Instant infrastructure** — spin up Kafka, PostgreSQL, or any service with a single command, no manual installation needed
- **Isolated environments** — each service runs in its own container without interfering with the rest of your system
- **Easy teardown** — stop and remove containers without leaving traces on your machine
- **Industry standard** — used across development, CI/CD pipelines, and production deployments

### How This Course Uses Docker

This course relies on Docker throughout:

- **Infrastructure components** — Kafka and PostgreSQL run as Docker containers, so you do not need to install or configure them manually
- **Consistent setup** — every student works with the same versions and configuration regardless of their OS
- **Dockerizing the application** — later in the course, we will also package and run our Spring Boot application itself as a Docker container

Docker is not optional for this course. Install it before proceeding to the next section.

---

<!-- TOC -->
* [Docker Installation Guide](#docker-installation-guide)
  * [What is Docker and Why Do You Need It?](#what-is-docker-and-why-do-you-need-it)
    * [Key Benefits](#key-benefits)
    * [How This Course Uses Docker](#how-this-course-uses-docker)
  * [Mac](#mac)
    * [Install via Homebrew (Recommended)](#install-via-homebrew-recommended)
    * [Requirements](#requirements)
    * [Steps (Manual Install)](#steps-manual-install)
  * [Windows](#windows)
    * [Requirements](#requirements-1)
    * [Steps](#steps)
    * [Alternative: Use Hyper-V (without WSL 2)](#alternative-use-hyper-v-without-wsl-2)
  * [Linux](#linux)
    * [Requirements](#requirements-2)
    * [Steps (Ubuntu / Debian)](#steps-ubuntu--debian)
    * [Steps (Fedora / RHEL)](#steps-fedora--rhel)
  * [Verify Docker Compose](#verify-docker-compose)
  * [Quick Sanity Check](#quick-sanity-check)
<!-- TOC -->

## Mac

### Install via Homebrew (Recommended)

```bash
brew install --cask docker
```

### Requirements
- macOS 12 (Monterey) or later
- Apple Silicon (M1/M2/M3) or Intel chip

### Steps (Manual Install)

1. **Download Docker Desktop**

   Go to [https://www.docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop) and download the installer for your chip:
   - **Apple Silicon:** Download the `Apple Chip` version
   - **Intel:** Download the `Intel Chip` version

2. **Install Docker Desktop**

   - Open the downloaded `.dmg` file
   - Drag the Docker icon to the Applications folder
   - Open Docker from Applications

3. **Grant Permissions**

   Docker will prompt for your password to install helper components. Allow it.

4. **Verify Installation**

   Open Terminal and run:
   ```bash
   docker --version
   docker run hello-world
   ```

---

## Windows

### Requirements
- Windows 10 64-bit (version 1903 or later) or Windows 11
- WSL 2 (Windows Subsystem for Linux 2) enabled

### Steps

1. **Enable WSL 2**

   Open PowerShell as Administrator and run:
   ```powershell
   wsl --install
   ```
   Restart your machine when prompted.

2. **Download Docker Desktop**

   Go to [https://www.docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop) and download the Windows installer.

3. **Run the Installer**

   - Double-click `Docker Desktop Installer.exe`
   - Ensure "Use WSL 2 instead of Hyper-V" is checked
   - Follow the prompts and restart when complete

4. **Verify Installation**

   Open a new terminal and run:
   ```bash
   docker --version
   docker run hello-world
   ```

### Alternative: Use Hyper-V (without WSL 2)

If you prefer not to use WSL 2, Docker Desktop can run on Hyper-V instead.

1. **Enable Hyper-V**

   Open PowerShell as Administrator and run:
   ```powershell
   Enable-WindowsOptionalFeature -Online -FeatureName Microsoft-Hyper-V -All
   ```
   Restart your machine.

2. **Install Docker Desktop**

   During installation, leave "Use WSL 2 instead of Hyper-V" **unchecked**.

3. **Verify Installation**

   ```bash
   docker --version
   docker run hello-world
   ```

> **Note:** Hyper-V requires Windows 10/11 Pro, Enterprise, or Education. It is not available on Windows Home. WSL 2 is recommended for most users as it works on all editions and offers better performance.

---

## Linux

### Requirements
- 64-bit Linux distribution (Ubuntu, Debian, Fedora, or RHEL recommended)
- `sudo` access

### Steps (Ubuntu / Debian)

1. **Remove old versions**

   ```bash
   sudo apt remove docker docker-engine docker.io containerd runc
   ```

2. **Set up the Docker repository**

   ```bash
   sudo apt update
   sudo apt install -y ca-certificates curl gnupg

   sudo install -m 0755 -d /etc/apt/keyrings
   curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
   sudo chmod a+r /etc/apt/keyrings/docker.gpg

   echo \
     "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
     https://download.docker.com/linux/ubuntu \
     $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
     sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
   ```

3. **Install Docker Engine**

   ```bash
   sudo apt update
   sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
   ```

4. **Run Docker without sudo (optional but recommended)**

   ```bash
   sudo usermod -aG docker $USER
   newgrp docker
   ```

5. **Start and enable Docker**

   ```bash
   sudo systemctl start docker
   sudo systemctl enable docker
   ```

6. **Verify Installation**

   ```bash
   docker --version
   docker run hello-world
   ```

### Steps (Fedora / RHEL)

1. **Install Docker Engine**

   ```bash
   sudo dnf install -y dnf-plugins-core
   sudo dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo
   sudo dnf install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
   ```

2. **Start and enable Docker**

   ```bash
   sudo systemctl start docker
   sudo systemctl enable docker
   ```

3. **Run Docker without sudo (optional but recommended)**

   ```bash
   sudo usermod -aG docker $USER
   newgrp docker
   ```

4. **Verify Installation**

   ```bash
   docker --version
   docker run hello-world
   ```

---

## Verify Docker Compose

Docker Desktop (Windows and Mac) includes Docker Compose out of the box. On Linux, it is installed as a plugin with the commands above.

Verify with:

```bash
docker compose version
```

---

## Quick Sanity Check

Run the following to confirm everything is working:

```bash
docker run hello-world
```

You should see:

```
Hello from Docker!
This message shows that your installation appears to be working correctly.
```
