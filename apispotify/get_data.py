import datetime as dt 
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials #To access authorised Spotify data
import pandas as pd

client_id = 'd3afecf419a4429a857fe974f1f99971'
client_secret = 'a54b1a335b4e4948a8b2533b5348ac58'
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager) #spotify object to access API
now = dt.datetime.now() #- dt.timedelta(days=7)
current_time = now.strftime('%Y-%m-%dT%H:%M:%S')

# write data in csv file
def sp_to_csv(data_full,csv_name):
    """
    Save data in csv format
    """
    dic_full= {}
    for key in data_full[0].keys() : 
        dic_full[key] = []

    for record in data_full: 
        for k,v in record.items():
            dic_full[k].append(v)

    df = pd.DataFrame.from_dict(dic_full)
    df.to_csv(csv_name)

def launch(path):
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


    sp_to_csv(playlists_featured_full,"{}/df_playlists_featured_full.csv".format(path))
    sp_to_csv(tracks_featured_full, "{}/df_tracks_featured_full.csv".format(path))
    sp_to_csv(artists_featured_full,"{}/df_artists_featured_full.csv".format(path))


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


    sp_to_csv(albums_releases_full, "{}/df_albums_releases_full.csv".format(path))
    sp_to_csv(tracks_releases_full, "{}/df_tracks_releases_full.csv".format(path))
    sp_to_csv(artists_releases_full, "{}/df_artists_releases_full.csv".format(path))