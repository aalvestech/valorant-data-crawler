# valorant-data-crawler

 Repository valorant-data-crawler: Collects data from specific player's matches in Valorant. Uses TrackGG API to fetch results, statistics, and relevant metrics. Developed to centralize code, enabling collaboration and improvements. Contributions are welcome.

## Prerequisites

Before running the crawler, ensure you have the following:

- Python 3.x installed
- [Docker Desktop](https://desktop.docker.com/win/main/amd64/Docker%20Desktop%20Installer.exe?utm_source=docker&utm_medium=webreferral&utm_campaign=dd-smartbutton&utm_location=module)
- Having an AWS cloud service account with the possibility of having all services in the free tier. [Link](https://aws.amazon.com/pt/console/)
- Having an IAM user with read and write permissions on S3 buckets. [Link](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users.html)
- To create an S3 bucket with a name of your choice, follow these steps. Make sure to update the bucket name in the code within the AWS modules. [Link](https://docs.aws.amazon.com/pt_br/AmazonS3/latest/userguide/creating-buckets-s3.html)
- Your S3 bucket should have the following folder structure:
<pre>
```
├── valorant/
│   ├── raw/
│   │   ├── summary/
│   │   ├── matches/
│   │   ├── details/
│   ├── cleaned/
│   │   ├── details/
│   │   ├── metadata/
│   │   ├── player_loadout/
│   │   ├── player_round_damage/
│   │   ├── player_round_kills/
│   │   ├── player_round/
│   │   ├── player_summary/
│   │   ├── round_summary/
│   │   ├── team_summary/
```
</pre>
## Installation

1. Clone this repository: `https://github.com/aalvestech/valorant-data-crawler`
2. Navigate to the project directory: `cd valorant-data-crawler`
3. Install required dependencies: `pip install -r requirements.txt`
4. Run the `docker-compose up` command in your terminal. Make sure you are in the project's root directory or provide the path to where the `docker-compose.yml` file is located."

## Folder Schema

<pre>
```
valorant-data-crawler
│   .env
│   .gitignore
│   chromedriver.exe
│   docker-compose.yml
│   README.md
│   requirements.txt
│
├───docker
│       Dockerfile
│
└───src
    │   crawler.py
    │   main.py
    │   __init__.py
    │
    ├───aws
    │   │   aws.py
    │   │   __init__.py
    │
    ├───data_clean
    │   │   data_cleaner.py
    │   │   make_cleaned.py
    │   │   __init__.py
    │
    ├───raw_data
    │   │   get_raw_data.py
    │   │   __init__.py
```
</pre>

- PS: Your repository should have the same folder structure as shown above.

## Configuration

1. Create the `.env` file at the root of the repository you cloned in step 1 of the installation.
2. Inside your `.env` file, insert the information about your AWS IAM user
   - `AWS_ACCESS_KEY_ID `: Your IAM access key.
   - `AWS_SECRET_ACCESS_KEY `: Your IAM secret access key.

## Usage

1. Run the crawler: `python -m src.main`
2. The crawler will launch a Chrome browser window.
3. The extracted data will be saved in s3. Each file in its respective `raw/` or `cleaned/` stage and in their respective folders within the S3 bucket.

## Contributing

Contributions are welcome! If you find any issues or have suggestions for improvement, please open an issue or submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE).