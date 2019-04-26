<img src="https://theventure.city/wp-content/uploads/2017/06/Theventurecity-logoweb-1.png" >

# Set up a Google Service Account
## A two-minute primer to configuration your Google account and Google Sheets to enable writing the results of the Python scripts with server-side authentication

To set up a service account and download a credentials file, follow these instructions (steps 1-6 are shown in the gif below):
1. Go to the [Google APIs Console](https://console.developers.google.com) (see first gif below for steps 1-6)
1. Create a new project
1. Click Enable API, then search for and enable the Google Sheets API
1. Create credentials for a Web Server to access Application Data
1. Name the service account and grant it a Project Role of Editor
1. Download the JSON file (Caution! This can happen with little warning)

<img src="https://s3.amazonaws.com/com.twilio.prod.twilio-docs/original_images/google-developer-console.gif" >

1. Copy the JSON file to your code directory and refer to it by name from [config.ini](python/config.ini)
1. Find the client_email inside the json file. Back in the Google Sheets spreadsheet that you want to write to, click the Share button in the top right, paste the client email into the People field to give it edit rights, and hit Send (see second gif below)

<img src="https://s3.amazonaws.com/com.twilio.prod.twilio-docs/original_images/share-google-spreadshet.gif" >

## Credits
This borrows heavily from [Greg Baugues's post on Twilio.com](https://www.twilio.com/blog/2017/02/an-easy-way-to-read-and-write-to-a-google-spreadsheet-in-python.html)
