# SA Tech Job Analyzer

This project is a full-stack application designed to analyze the tech job market in South Africa. It scrapes job data, cleans it, and presents it through a web interface.

## Project Structure

The project is divided into three main components:

-   `src/Api`: An ASP.NET Core Web API that serves the cleaned job data.
-   `src/Client`: A React frontend that provides a user interface to view the job data.
-   `src/DataCleaner`: A Python script that scrapes and cleans the job data.

## Getting Started

### Prerequisites

-   .NET 10 SDK
-   Node.js and npm
-   Python 3 and pip

### Installation

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/devzwide/sa-tech-job-analyzer.git
    cd sa-tech-job-analyzer
    ```

2.  **API Setup:**

    ```bash
    cd src/Api
    dotnet restore
    ```

3.  **Data Cleaner Setup:**

    ```bash
    cd src/DataCleaner
    python -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

4.  **Client Setup:**

    ```bash
    cd src/Client
    npm install
    ```

## Usage

1.  **Run the Data Cleaner:**

    ```bash
    cd src/DataCleaner
    source venv/bin/activate
    python clean_data.py
    ```

2.  **Run the API:**

    ```bash
    cd src/Api
    dotnet run
    ```

3.  **Run the Client:**

    ```bash
    cd src/Client
    npm run dev
    ```

The client will be available at `http://localhost:5173`.

## Contributing

Contributions are welcome! Please feel free to submit a pull request.

## License

This project is licensed under the MIT License.
