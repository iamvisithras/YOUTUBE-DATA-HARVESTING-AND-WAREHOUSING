from googleapiclient.discovery import build
import pandas as pd
import mysql.connector
import streamlit as st
import datetime



#API key connection

def Api_connect():
    Api_Id="AIzaSyDXWRYkw9O_X1qIuZXcO00KWuPLHsU2sj4"

    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=Api_Id)
    return youtube

youtube=Api_connect()

#get channels information
def get_channel_info(channel_id):
    request=youtube.channels().list(
                        part="snippet,ContentDetails,statistics",
                        id=channel_id
    )
    response=request.execute()
    data = []
    for i in response['items']:
                  data.append({"Channel_Name": i["snippet"]["title"],
                        "Channel_Id": i["id"],
                        "Subscribers": i["statistics"]["subscriberCount"],
                        "Views": i["statistics"]["viewCount"],
                        "Total_videos": i["statistics"]["videoCount"],
                        "Channel_description": i["snippet"]["description"],
                        "Playlist_Id": i["contentDetails"]["relatedPlaylists"]["uploads"]})
    return data           

    channel_details=get_channel_info(channel_id)


    #get video ids

def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                        part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response=youtube.playlistItems().list(
                                            part="snippet",
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response['items'])):                                   
            video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])  
            next_page_token=response.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids                          

    videos_ids=get_videos_ids(channel_id)

    #get video information
def get_video_info(video_ids):
    Video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        for item in response["items"]:
            publish_date_str = item['snippet']['publishedAt']
            publish_date = datetime.datetime.strptime(publish_date_str, '%Y-%m-%dT%H:%M:%SZ')
            formatted_publish_date = publish_date.strftime('%Y-%m-%d %H:%M:%S')


            data = {
                'Channel_Name': item['snippet']['channelTitle'],
                'channel_Id': item['snippet']['channelId'],
                'Video_Id': item['id'],
                'Title': item['snippet']['title'],
                'Tags': item['snippet'].get('tags'),
                'Thumbnail': item['snippet']['thumbnails']['default']['url'],
                'Description': item['snippet'].get('description'),
                'Publishdate': formatted_publish_date,
                'Duration': item['contentDetails']['duration'],
                'Views': item['statistics'].get('viewCount'),
                'Likes': item['statistics'].get('likeCount'),
                'Comments': item['statistics'].get('commentCount'),
                'Favorite_count': item['statistics'].get('favoriteCount'),
                'Definition': item['contentDetails']['definition'],
                'Caption_Status': item['contentDetails']['caption']
            }
            Video_data.append(data)
    return Video_data

    video_details=get_video_details(videos_ids)

    #get comment information
def get_comment_info(video_ids):
    Comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            for item in response['items']:
                publish_date_str = item['snippet']['topLevelComment']['snippet']['publishedAt']
                publish_date = datetime.datetime.strptime(publish_date_str, '%Y-%m-%dT%H:%M:%SZ')
                formatted_publish_date = publish_date.strftime('%Y-%m-%d %H:%M:%S')
                data = {
                    'Comment_id': item['snippet']['topLevelComment']['id'],
                    'Video_id': item['snippet']['topLevelComment']['snippet']['videoId'],
                    'Comment_text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'Comment_Author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'Comment_Published':  formatted_publish_date
                }
                Comment_data.append(data)
    except Exception as e:
        print("Error retrieving comments:", e)
    return Comment_data

    comment_details=get_comment_info(videos_ids)

    st.dataframe(channel_details)

def main():
    with st.sidebar:
        st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
        st.header("Skill Take Away")
        st.caption("Python Scripting")
        st.caption("Data Collection")
        st.caption("MySQL")
        st.caption("API Integration")
        st.caption("Data Management using SQL")

    youtube = Api_connect()  
    channel_id_input_placeholder = 'channel_id_input'
    channel_id = st.text_input('Enter the channel ID', key=channel_id_input_placeholder)

    if st.button("Collect and Store Data"):        
                                        channel_details=get_channel_info(channel_id)
                                        videos_ids = get_videos_ids(channel_id)
                                        video_details=get_video_info(videos_ids)
                                        comment_details = get_comment_info(videos_ids)
        

                                        st.subheader("Channels")
                                        st.write(pd.DataFrame(channel_details))

                                        st.subheader("Videos")
                                        st.write(pd.DataFrame(video_details))

                                        st.subheader("Comments")
                                        st.write(pd.DataFrame(comment_details))


if __name__ == "__main__":
    main()


import mysql.connector


# MySQL connection configuration
mysql_host = "localhost"
mysql_user = "root"
mysql_password = "Your PASSWORD"
mysql_database = "YOUR DATABASE"
mysql_port = "YOUR PORT"
# Function to connect to MySQL database
def connect_to_mysql():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Svisithra",
            database="youtube_data",
            port="3306"
        )
        print("Connected to MySQL database successfully")
        return conn
    except mysql.connector.Error as e:
        print("Error connecting to MySQL database:", e)
        return None

# Function to create tables in SQL
def create_tables(conn):
    cursor = conn.cursor()

    # Table creation queries
    channels_table_query = """
    CREATE TABLE IF NOT EXISTS channels(
        Channel_Name varchar(225),
        Channel_Id varchar(225) primary key,
        Subscribers bigint,
        Views bigint,
        Total_Videos int,
        Channel_Description text,
        Playlist_Id varchar(225)
    )"""

    videos_table_query = """
    CREATE TABLE IF NOT EXISTS videos (
        Channel_Name varchar(225),
        Channel_Id varchar(225),
        Video_Id varchar(225) primary key,
        Title varchar(225),
        Tags text,
        Thumbnail varchar(2225),
        Description text,
        Published_Date timestamp,
        Duration VARCHAR(255),
        Views bigint,
        Likes bigint,
        Comments int,
        Favorite_Count int,
        Definition varchar(225),
        Caption_Status varchar(225)
    )"""

    comments_table_query = """
    CREATE TABLE IF NOT EXISTS comments (
        Comment_Id varchar(225) primary key,
        Video_Id varchar(225),
        Comment_Text text,
        Comment_Author varchar(225),
        Comment_Published timestamp
    )"""

    try:
        # Execute table creation queries
        cursor.execute(channels_table_query)
        cursor.execute(videos_table_query)
        cursor.execute(comments_table_query)
        conn.commit()
        print("Tables created successfully in MySQL")

    except mysql.connector.Error as e:
        print("Error creating tables in MySQL:", e)
        conn.rollback()

    finally:
        cursor.close()

# Test the functions
conn = connect_to_mysql()
if conn is not None:
    create_tables(conn)
    conn.close()

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Svisithra",
    database="youtube_data",
    port="3306"
)    

# Function to insert channel details into MySQL database

def insert_channel_info_to_mysql(conn, channel_info):
    cursor = conn.cursor()
    try:
        
        for info in channel_info:

            insert_query = """
            INSERT INTO channel_data (Channel_Name, Channel_Id, Subscribers, Views, Total_videos, Channel_description, Playlist_Id) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            # Execute the query with data from the channel_info list
            cursor.execute(insert_query, (info["Channel_Name"], info["Channel_Id"], info["Subscribers"], info["Views"], info["Total_videos"], info["Channel_description"], info["Playlist_Id"]))

        conn.commit()
        print("Channel info inserted into MySQL successfully!")
    except mysql.connector.Error as e:
        print("Error inserting channel info into MySQL:", e)
        conn.rollback()
    finally:
        cursor.close()
# Function to insert video data into MySQL database
def insert_video_data_to_mysql(conn, video_data):
    cursor = conn.cursor()
    try:
        for data in video_data:
            tags = data.get('Tags', [])
            if not isinstance(tags, list):
                tags = [tags]  
            tags = [tag for tag in tags if tag is not None]
            insert_query = """
            INSERT INTO Video_data (Channel_Name, Channel_Id, Video_Id, Title, Tags, Thumbnail, Description, Publishdate, Duration, Views, Likes, Comments, Favorite_count, Definition, Caption_Status) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            # Execute the query with data from the video_data list
            cursor.execute(insert_query, (
                data['Channel_Name'],
                data['channel_Id'],
                data['Video_Id'],
                data['Title'],
                ",".join(tags),  
                data['Thumbnail'],
                data.get('Description', ''),
                data['Publishdate'],
                data['Duration'],
                data['Views'],
                data.get('Likes', 0),  
                data.get('Comments', 0),  
                data.get('Favorite_count', 0),  
                data['Definition'],
                data['Caption_Status']
            ))
        
        # Commit the changes to the database
        conn.commit()
        print("Video data inserted into MySQL successfully!")
    except mysql.connector.Error as e:
        print("Error inserting video data into MySQL:", e)
        conn.rollback()
    finally:
        cursor.close()



import mysql.connector

# Function to insert comment data into MySQL database
def insert_comment_data_to_mysql(conn, comment_data):
    cursor = conn.cursor()
    try:
        for data in comment_data:
            insert_query = """
            INSERT INTO comment_data (Comment_id, Video_id, Comment_text, Comment_Author, Comment_Published) 
            VALUES (%s, %s, %s, %s, %s)
            """
            # Execute the query with data from the comment_data list
            cursor.execute(insert_query, (
                data['Comment_id'],
                data['Video_id'],
                data['Comment_text'],
                data['Comment_Author'],
                data['Comment_Published']
            ))
        
        # Commit the changes to the database
        conn.commit()
        print("Comment data inserted into MySQL successfully!")
    except mysql.connector.Error as e:
        print("Error inserting comment data into MySQL:", e)
        conn.rollback()
    finally:
        cursor.close()

# Button to migrate selected channel to SQL
def main():

    st.title(":red[MIGRATION TO SQL]")

    # Connect to YouTube API
    youtube = Api_connect()

    channel_id = st.text_input('Enter the Channel ID')

    if st.button("Migrate Data"):
        channel_data = get_channel_info(channel_id)
        video_ids = get_videos_ids(channel_id)
        video_data = get_video_info(video_ids)
        comment_data = get_comment_info(video_ids)


        # Creating tables if they don't exist
        if conn is not None:
            create_tables(conn)

            insert_channel_info_to_mysql(conn, channel_data)
            insert_video_data_to_mysql(conn, video_data)
            insert_comment_data_to_mysql(conn, comment_data)
        # Closing the MySQL connection
            conn.close()

            # Displaying success message
            st.success("Data migrated to MySQL successfully!")

# Run the main function
if __name__ == "__main__":
   main()

import plotly.express as px

def execute_query(query):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Svisithra",
        database="youtube_data",
        port="3306"
    )
    cursor = mydb.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    mydb.close()
    return data

st.title(":red[QUERIES AND RESULTS]")
question = st.selectbox("Select Your Question To Display The Query",
                        ("1.What are the names of all the videos and their corresponding channels?",
                        "2.Which channels have the most number of videos, and how many videos do they have?",
                        "3.What are the top 10 most viewed videos and their respective channels?",
                        "4.How many comments were made on each video, and what are their corresponding video names?",
                        "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                        "6.What is the total number of likes for each video, and what are their corresponding video names?",
                        "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                        "8.What are the names of all the channels that have published videos in the year 2022?",
                        "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                        "10.Which videos have the highest number of comments, and what are their corresponding channel names?"),
                        )

if question == "1.What are the names of all the videos and their corresponding channels?":
     query = """
        SELECT c.Channel_Name, v.Title 
        FROM channel_data AS c 
        JOIN video_data AS v 
        ON c.Channel_Id = v.Channel_Id;
    """
     result = execute_query(query)
     df = pd.DataFrame(result, columns=["Channel_Name","Title"])
     st.write(df)

elif question == "2.Which channels have the most number of videos, and how many videos do they have?":
        query = """
            SELECT Channel_Name, Total_videos 
            FROM channel_data
            ORDER BY Total_videos DESC;
        """
        result = execute_query(query)
        df1 = pd.DataFrame(result, columns=["Channel_Name", "Total_videos"])
        st.write(df1)

        fig = px.bar(df1, x='Channel_Name', y='Total_videos', title='Channels with Most Videos',
                 labels={'Total_videos': 'Number of Videos', 'Channel_Name': 'Channel'})

    # Update layout
        fig.update_layout(xaxis_title='Channel', yaxis_title='Number of Videos')

        # Display the chart using Streamlit
        st.write("Visualization:")
        st.plotly_chart(fig)

elif question == "3.What are the top 10 most viewed videos and their respective channels?":
        query = """
                select Channel_Name,Title,Views from video_data
                order by Views desc limit 10;
                """
        result = execute_query(query)
        df2 = pd.DataFrame(result, columns=["Channel_Name", "Title","Views"])
        st.write(df2)

        fig = px.bar(df2, x='Channel_Name', y='Views', text='Title', title='Top 10 Most Viewed Videos by Channel')
        fig.update_traces(textposition='outside')
        fig.update_layout(xaxis_title='Channel Name', yaxis_title='Views')
        st.write("Visualization:")
        st.plotly_chart(fig)

elif question == "4.How many comments were made on each video, and what are their corresponding video names?" :
        query = """
                Select c.Channel_Name, v.comments,v.Title from channel_data as c join video_data as v on c.Channel_ID=v.Channel_ID;
                """
        result = execute_query(query)
        df3 = pd.DataFrame(result, columns=["Channel_Name", "Comments","Title"])
        st.write(df3)

        fig = px.bar(df3, x='Title', y='Comments', color='Channel_Name', title='Number of Comments on Each Video')
        fig.update_layout(xaxis_title='Video Title', yaxis_title='Number of Comments', legend_title='Channel Name')
        st.write("Visualization:")
        st.plotly_chart(fig)

elif question == "5.Which videos have the highest number of likes, and what are their corresponding channel names?" :
        query = """
                select c.Channel_Name,v.Title,v.Likes from channel_data as c join video_data as v on c.Channel_ID=v.Channel_ID
                order by v.Likes desc;
                """
        result = execute_query(query)
        df4 = pd.DataFrame(result, columns=["Channel_Name", "Title","Likes"])
        st.write(df4)

        fig = px.bar(df4, x='Title', y='Likes', color='Channel_Name', title='Number of Likes on Each Video')
        fig.update_layout(xaxis_title='Video Title', yaxis_title='Number of Likes', legend_title='Channel Name')
        st.write("Visualization:")
        st.plotly_chart(fig)

elif question == "6.What is the total number of likes for each video, and what are their corresponding video names?" :
        query = """
                SELECT Title, SUM(Likes) AS Total_Likes
                FROM video_data
                GROUP BY Title
                ORDER BY Total_Likes DESC;
                """
        result = execute_query(query)
        df5 = pd.DataFrame(result, columns=[ "Title","Likes"])
        st.write(df5)

        fig = px.bar(df5, x='Title', y='Likes', title='Total Number of Likes for Each Video')
        fig.update_layout(xaxis_title='Video Title', yaxis_title='Total Number of Likes')
        st.write("Visualization:")
        st.plotly_chart(fig)

elif question == "7.What is the total number of views for each channel, and what are their corresponding channel names?" :
        query = """
                select Channel_Name,views from channel_data order by views desc;
                """
        result = execute_query(query)
        df6 = pd.DataFrame(result, columns=[ "Channel_Name","views"])
        st.write(df6)

        fig = px.bar(df6, x='Channel_Name', y='views', title='Total Number of Views for Each Channel')
        fig.update_layout(xaxis_title='Channel Name', yaxis_title='Total Number of Views')
        st.write("Visualization:")
        st.plotly_chart(fig)

elif question == "8.What are the names of all the channels that have published videos in the year 2022?" :
        query = """
                select Channel_Name from video_data
                where EXTRACT(YEAR FROM Publishdate) = 2022;
                """
        result = execute_query(query)
        df7 = pd.DataFrame(result, columns=[ "Channel_Name"])
        st.write(df7)

        fig = px.histogram(df7, x='Channel_Name', title='Channels that Published Videos in 2022')
        fig.update_layout(xaxis_title='Channel Name', yaxis_title='Frequency')
        st.write("Visualization:")
        st.plotly_chart(fig)

elif question == "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?" :
        query = """
                SELECT c.Channel_Name, AVG(v.Duration) AS Avg_Duration
                FROM channel_data c
                JOIN video_data v ON c.Channel_Id = v.Channel_Id
                GROUP BY c.Channel_Name;
                """
        result = execute_query(query)
        df8 = pd.DataFrame(result, columns=[ "Channel_Name","Avg_Duration"])
        st.write(df8)

        fig = px.bar(df8, x='Channel_Name', y='Avg_Duration', title='Average Duration of Videos in Each Channel')
        fig.update_layout(xaxis_title='Channel Name', yaxis_title='Average Duration')
        st.write("Visualization:")
        st.plotly_chart(fig)

elif question == "10.Which videos have the highest number of comments, and what are their corresponding channel names?" :
        query = """
                SELECT c.Channel_Name, v.Title, COUNT(co.Comment_Id) AS Num_Comments
                FROM channel_data c
                JOIN video_data v ON c.Channel_Id = v.Channel_Id
                LEFT JOIN comment_data co ON v.Video_Id = co.Video_Id
                GROUP BY c.Channel_Name, v.Title
                ORDER BY Num_Comments DESC
                LIMIT 10;
                """
        result = execute_query(query)
        df9 = pd.DataFrame(result, columns=[ "Channel_Name","Title","Num_Comments"])
        st.write(df9)

        fig = px.bar(df9, x='Title', y='Num_Comments', color='Channel_Name', title='Videos with the Highest Number of Comments')
        fig.update_layout(xaxis_title='Video Title', yaxis_title='Number of Comments', legend_title='Channel Name')
        st.write("Visualization:")
        st.plotly_chart(fig)
