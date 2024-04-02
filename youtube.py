from googleapiclient.discovery import build
import pandas as pd
import psycopg2
import streamlit as st

#API key connection

api_service_name = "youtube"
api_version = "v3"
api_key="AIzaSyDjhgrNmGkadcC8BRJVcK6adyDlHMi3I4Q"
youtube=build(api_service_name,api_version,developerKey=api_key)

#To get channel information

def get_channel_details(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    data = {"channel_name":response['items'][0]['snippet']['title'],
              "Channel_id":response['items'][0]["id"],
              "publishe_at":response['items'][0]['snippet']['publishedAt'],
              "playlist_id":response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
              "Subscribers":response['items'][0]['statistics']['subscriberCount'],
              "Total_count":response['items'][0]['statistics']['videoCount'],
              "Channel_description":response['items'][0]['snippet']['description']}
    return data


#To get Video_id

def get_video_ids(channel_id):

    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                     part='contentDetails').execute()
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=playlist_id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])

        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

                                    
#Get video_information

def get_video_info(video_ids):

    video_data=[]
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict (Channel_Name=item['snippet']['channelTitle'],
                        Channel_Id=item['snippet']['channelId'],
                        Video_Id=item['id'],
                        Title=item['snippet']['title'],
                        Tags=item.get('tags'),
                        Description=item.get('description'),
                        Published_At=item['snippet']['publishedAt'],
                        Duration=item['contentDetails']['duration'],
                        Views=item.get('viewCount'),
                        Likes=item['statistics'].get('likecount'),
                        Comments=item.get('commentCount'),
                        Favorite_count=item['statistics']['favoriteCount'],
                        Definition=item['contentDetails']['definition'],
                        Caption_status=item['contentDetails']['caption']
                        )
            video_data.append(data)
    
    return video_data
#channel=get_channel_details

#Get comment information

def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                                            part="snippet",
                                            videoId =video_id,
                                            maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(comment_id=item['snippet']['topLevelComment']['id'],
                        video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)           
    except:
        pass
    return Comment_data

#Get playlist details

def get_playlist_details(channel_id):

        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()
                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                                Title=item['snippet']['title'],
                                channel_id=item['snippet']['channelId'],
                                channel_name=item['snippet']['channelTitle'],
                                PublishedAt=item['snippet']['publishedAt'],
                                video_count=item['contentDetails']['itemCount'])
                        
                        All_data.append(data) 

                next_page_token=response.get('next_page_token') 
                if next_page_token is None:

                        break
        return All_data

#Table creation for channel details

mydb=psycopg2.connect(host="localhost",
                      user="postgres",
                      password="praisy@1998",
                      database="youtube_data",
                      port="5432")
cursor=mydb.cursor()

try:
    create_query='''create table if not exists Channels(channel_name varchar(100),
    Channel_id varchar(80),
    publishe_at varchar(100),
    playlist_id varchar(100),
    Subscribers varchar(100),
    Total_count varchar(100),
    Channel_description text)'''

    cursor.execute(create_query)
    mydb.commit()

except:
    print("Channels table already created")

#Table creation for video details

mydb=psycopg2.connect(host="localhost",
                      user="postgres",
                      password="praisy@1998",
                      database="youtube_data",
                      port="5432")
cursor=mydb.cursor()

try:


    create_query='''create table if not exists videos(Channel_Name varchar(100),
        Channel_Id varchar(80),
        Video_Id varchar(100),
        Title varchar(100),
        Tags varchar(100),
        Description text,
        Published_At timestamp,
        Duration interval,
        Views varchar(100),
        Likes bigint,
        Comments text,
        Favorite_count varchar(100),
        Definition varchar(100),
        Caption_status varchar(100))'''

    cursor.execute(create_query)
    mydb.commit()

except:
    print("videos table already created")

#Table creation for comments details

mydb=psycopg2.connect(host="localhost",
                      user="postgres",
                      password="praisy@1998",
                      database="youtube_data",
                      port="5432")
cursor=mydb.cursor()

try:

    create_query='''create table if not exists comments(comment_id varchar(100),
        video_Id varchar(200),
        Comment_Text text,
        Comment_Author text,
        Comment_Published timestamp)'''
        

    cursor.execute(create_query)
    mydb.commit()

except:
    print("comments table already created")

# insert values into channel table

def insert_channels(channel):

    cursor=mydb.cursor()

    sql = "INSERT INTO Channels(channel_name,Channel_id,publishe_at,playlist_id,Subscribers,Total_count,Channel_description) VALUES (%s,%s,%s,%s,%s,%s,%s)"
    values= tuple (channel.values())

    cursor.execute(sql, values)

    mydb.commit()

    print(cursor.rowcount, "record inserted.") 

#insert values into videos table
def insert_videos(vid):
    mydb=psycopg2.connect(host="localhost",
                      user="postgres",
                      password="praisy@1998",
                      database="youtube_data",
                      port="5432")
    cursor=mydb.cursor()

    #converting dict into tuple
    video=[]
    for i in vid:
        tuple(i.values())
        video.append(tuple(i.values()))
        

    sql = "INSERT INTO Videos (Channel_Name,Channel_Id,Video_Id,Title,Tags,Description,Published_At,Duration,Views,Likes,Comments,Favorite_count,Definition,Caption_status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    val = video
    for i in video:

        cursor.execute(sql,i)

        mydb.commit()

    print(cursor.rowcount, "was inserted.")

#insert values into comments table


def insert_comments(com):
    mydb=psycopg2.connect(host="localhost",
                      user="postgres",
                      password="praisy@1998",
                      database="youtube_data",
                      port="5432")
    cursor=mydb.cursor()

    comment=[]
    for i in com:
            tuple(i.values())
            comment.append(tuple(i.values()))
    

    sql = "INSERT INTO comments(comment_id,video_Id,Comment_Text,Comment_Author,Comment_Published) VALUES (%s,%s,%s,%s,%s)"
    for i in comment:
        print(i)

        cursor.execute(sql,i)

        mydb.commit()

    print(cursor.rowcount, "was inserted.")


# Streamlit coding

with st.sidebar:
    st.title(":yellow[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data collection")
    st.caption("API Integration")
    st.caption("Data Management using Python and SQL")

channel_id=st.text_input("Enter the channel ID")
if st.button("Collect data from Youtube"):

    channel_data = get_channel_details(channel_id)
    Video_Ids=get_video_ids(channel_id)
    video_details=get_video_info(Video_Ids)
    comment_details=get_comment_info(Video_Ids)
    playlist_details=get_playlist_details(channel_id)

    #if show_table=="CHANNELS":
    insert_channels(channel_data)
    #elif show_table=="VIDEOS":
    insert_videos(video_details)
    #elif show_table=="COMMENTS":
    insert_comments(comment_details)

#SQL Connection

mydb=psycopg2.connect(host="localhost",
                      user="postgres",
                      password="praisy@1998",
                      database="youtube_data",
                      port="5432")
cursor=mydb.cursor()

question=st.selectbox("Select your question",("1. All the videos and channels name ",
                                              "2. Channels with most number of videos",
                                              "3. Top 10 most viewed videos",
                                              "4. Comments in each videos",
                                              "5. Videos with highesh likes",
                                              "6. Likes of all video",
                                              "7. Views of each channel",
                                              "8. Video published in the year of 2022",
                                              "9. average duration of all videos in each channel",
                                              "10. Video with highest number of comments"))
if question=="1. All the videos and channels name":

    query1='''select title as Videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif question=="2. Channels with most number of videos":
    query2='''select channel_name as channelname,total_count as no_videos from channels order by total_count desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)

elif question=="3.Top 10 most viewed videos":
    query3='''select views as views,channel_name as channelname,title as videotitle from videos where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)

elif question=="4. Comments in each videos":

    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no.of comments","videotitle"])
    st.write(df4)

elif question=="5. Videos with highesh likes":

    query5='''select title as videotitle,channel_name as channelname,likes as likecount from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question=="6. Likes of all video":

    query6='''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)

elif question=="7. Views of each channel":

    query7='''select channel_name as channelname,views as total_count from videos'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel_name","totalviews"])
    st.write(df7)

elif question=="8. Video published in the year of 2022":
    query8='''select title as video_title,published_at as videorelease,channel_name as channelname from videos where extract(year from published_at)=2022'''
    cursor.execute(query8)
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","published_at","channelname"])
    st.write(df8)

elif question=="9. average duration of all videos in each channel":

    query9='''SELECT channel_name AS channelname,AVG(EXTRACT(EPOCH FROM duration::interval)) AS average_duration FROM
        videos GROUP BY channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])
    df9

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)

elif question=="10. Video with highest number of comments":

    query10='''select title as videotitle,channel_name as channelname,comments as comments from videos where comments is not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
    st.write(df10)