import datetime as dt 
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials #To access authorised Spotify data
import pandas as pd
import os

client_id = 'd3afecf419a4429a857fe974f1f99971'
client_secret = 'a54b1a335b4e4948a8b2533b5348ac58'
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager) #spotify object to access API

now = dt.datetime.now() #- dt.timedelta(days=7)

current_time = now.strftime('%Y-%m-%dT%H:%M:%S')
print("Current Time =", current_time)

# Get all playlists featured
offset = -50
playlists_len= 50
playlists_featured_full = list()
tracks_featured_full = list()
artists_featured_full = list()
tmp_artist_ids = list()

while playlists_len==50 : 
    offset+=50
    playlists_featured_info=sp.featured_playlists(locale='FR', country='FR', timestamp=current_time, limit=50, offset=offset)
    playlists_len=len(playlists_featured_info)
    playlists_items= playlists_featured_info['playlists']['items']
    #Store playlists object full  
    for item in playlists_items:
        playlists_featured_full.append(sp.playlist(item['id'], fields=None, market='FR', additional_types=('track', )))
    #Store tracks object full and artist object full in separat list
    for playlist in playlists_featured_full: 
        for item in playlist['tracks']['items'] :
            if item['track']== None : 
                continue
            tracks_featured_full.append(sp.track(item['track']['id']))
            for artist_info in item['track']['artists']:
                if artist_info['id'] in tmp_artist_ids:
                    continue
                artists_featured_full.append(sp.artist(artist_info['id']))
                tmp_artist_ids.append(artist_info['id'])


dic_playlists_featured_full= {}
for key in playlists_featured_full[0].keys() : 
    dic_playlists_featured_full[key] = []



dic_tracks_featured_full= {}
for key in tracks_featured_full[0].keys() : 
    dic_tracks_featured_full[key] = []


dic_artists_featured_full= {}
for key in artists_featured_full[0].keys() : 
    dic_artists_featured_full[key] = []


for playlist in playlists_featured_full: 
    for k,v in playlist.items():
        dic_playlists_featured_full[k].append(v)

df = pd.DataFrame.from_dict(dic_playlists_featured_full)
df.to_csv("df_playlists_featured_full.csv")

for track in tracks_featured_full: 
    for k,v in track.items():
        dic_tracks_featured_full[k].append(v)

df = pd.DataFrame.from_dict(dic_tracks_featured_full)
df.to_csv("df_tracks_featured_full.csv")

for album in artists_featured_full: 
    for k,v in album.items():
        dic_artists_featured_full[k].append(v)

df = pd.DataFrame.from_dict(dic_artists_featured_full)
df.to_csv("df_artists_featured_full.csv")


offset = -50
albums_len= 50
albums_releases_full = list()
tracks_releases_full = list()
artists_releases_full = list()
tmp_artist_ids = list()

while albums_len==50 :
    offset+=50
    # Get all albums releases
    albums_releases = sp.new_releases(country='FR', limit=50, offset=offset)
    albums_len=len(albums_releases)
    albums_items = albums_releases['albums']['items']
    #Store albums object full 
    for item in albums_items:
        albums_releases_full.append(sp.album(item['id']))

    #Store tracks full data and artist full data
    for album in albums_releases_full: 
        for item in album['tracks']['items'] : 
            tracks_releases_full.append(sp.track(item['id'])) 
        for artist_info in album['artists']:
            if artist_info['id'] in tmp_artist_ids:
                continue
            artists_releases_full.append(sp.artist(artist_info['id']))
            tmp_artist_ids.append(artist_info['id'])



dic_albums_releases_full= {}
for key in albums_releases_full[0].keys() : 
    dic_albums_releases_full[key] = []



dic_tracks_releases_full= {}
for key in tracks_releases_full[0].keys() : 
    dic_tracks_releases_full[key] = []


dic_artists_releases_full= {}
for key in artists_releases_full[0].keys() : 
    dic_artists_releases_full[key] = []


for album in albums_releases_full: 
    for k,v in album.items():
        dic_albums_releases_full[k].append(v)

df = pd.DataFrame.from_dict(dic_albums_releases_full)
df.to_csv("df_albums_releases_full.csv")

for track in tracks_releases_full: 
    for k,v in track.items():
        dic_tracks_releases_full[k].append(v)

df = pd.DataFrame.from_dict(dic_tracks_releases_full)
df.to_csv("df_tracks_releases_full.csv")

for artist in artists_releases_full: 
    for k,v in artist.items():
        dic_artists_releases_full[k].append(v)

df = pd.DataFrame.from_dict(dic_artists_releases_full)
df.to_csv("df_artists_releases_full.csv")
os.system("hdfs dfs -moveFromLocal ./*.csv /user/iabd2_group6/input/")
