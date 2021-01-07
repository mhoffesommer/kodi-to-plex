#!/usr/bin/python
#
# Hacked together script that moves watched items from Kodi to a single Plex user
# https://github.com/mhoffesommer/kodi-to-plex

import sqlite3,os,re
from datetime import datetime
from imdb import IMDb

# filename of source Kodi DB
KODI_DB_FN='MyVideos104.db'

# filename of target Plex DB
PLEX_DB_FN='com.plexapp.plugins.library.db'

# ID of target account in plex DB
PLEX_ACCT_ID=1 

print('Reading Kodi DB...')
ia=IMDb()
db=sqlite3.connect(KODI_DB_FN)

seen_movies=[] # tuple: IMDB ID, datetime
seen_episodes=[] # tuple: tvdb ID, datetime, season, episode

played=db.execute("select idFile,lastPlayed,strFilename from files where lastPlayed is not null")
for cur in played:
    dt=datetime.fromisoformat(cur[1])
    bookmark=db.execute("select timeInSeconds,totalTimeInSeconds from bookmark where idFile=?",(cur[0],)).fetchone()
    movie=db.execute("select c00 as title,c07 as year,c09 as imdb_id from movie where idFile=?",(cur[0],)).fetchone()
    if movie:
        if movie[2]:
            seen_movies.append((movie[2],dt))
            continue
        i=ia.search_movie('%s (%s)'%(movie[0],movie[1]))
        if not i:
            i=ia.search_movie(movie[0])
        seen_movies.append(('tt%s'%i[0].movieID,dt))
        continue
    episode=db.execute("select idShow,c12 as season,c13 as episode from episode where idFile=?",(cur[0],)).fetchone()
    if episode:
        show=db.execute("select c00 as title,c10 from tvshow where idShow=?",(episode[0],)).fetchone()
        m=re.search('&quot;id&quot;:([\d]+)}',show[1])
        if not m:
            m=re.search('/series/([\d]+)/',show[1])
        if not m:
            continue
        seen_episodes.append((m[1],dt,episode[1],episode[2]))
        continue
db.close()
print('Found %d watched movies, %d watched episodes'%(len(seen_movies),len(seen_episodes)))

print('Adding view items to Plex DB...')
db=sqlite3.connect(PLEX_DB_FN)
db.row_factory=sqlite3.Row

for cur in seen_movies:
    movie=db.execute('select * from metadata_items where guid=?',('com.plexapp.agents.imdb://%s?lang=en'%cur[0],)).fetchall()
    if len(movie)!=1:
        print('  Movie entry for',cur[0],'not found in Plex DB - skipping')
        continue

    movie=movie[0]
    if db.execute('select * from metadata_item_views where account_id=? and guid=?',(PLEX_ACCT_ID,movie['guid'])).fetchall():
        continue

    print('Movie:',movie['title'])
    db.execute('insert into metadata_item_views(account_id,guid,metadata_type,library_section_id,grandparent_title,'+
            'parent_index,"index",title,thumb_url,viewed_at,grandparent_guid,originally_available_at) values (?,?,?,?,?,?,?,?,?,?,?,?)',
        (PLEX_ACCT_ID,
            movie['guid'],
            movie['metadata_type'],
            movie['library_section_id'],
            '',
            -1,
            1,
            movie['title'],
            movie['user_thumb_url'],
            cur[1],
            '',
            movie['originally_available_at']))
    db.commit()

for cur in seen_episodes:
    episode=db.execute('select * from metadata_items where guid=?',('com.plexapp.agents.thetvdb://%s/%s/%s?lang=en'%(cur[0],cur[2],cur[3]),)).fetchall()
    if not episode:
        episode=db.execute('select * from metadata_items where guid like ?',('com.plexapp.agents.thetvdb://%s/%s/%s?%%'%(cur[0],cur[2],cur[3]),)).fetchall()
    if len(episode)!=1:
        print('  TV entry for show',cur[0],'not found in Plex DB - skipping')
        continue

    episode=episode[0]
    if db.execute('select * from metadata_item_views where account_id=? and guid=?',(PLEX_ACCT_ID,episode['guid'])).fetchall():
        continue

    season=db.execute('select * from metadata_items where id=?',(episode['parent_id'],)).fetchone()
    show=db.execute('select * from metadata_items where id=?',(season['parent_id'],)).fetchone()
    print('TV episode:',show['title'],episode['title'])
    db.execute('insert into metadata_item_views(account_id,guid,metadata_type,library_section_id,grandparent_title,'+
            'parent_index,"index",title,thumb_url,viewed_at,grandparent_guid,originally_available_at) values (?,?,?,?,?,?,?,?,?,?,?,?)',
        (PLEX_ACCT_ID,
            episode['guid'],
            episode['metadata_type'],
            episode['library_section_id'],
            show['title'],
            cur[2],
            cur[3],
            episode['title'],
            episode['user_thumb_url'],
            cur[1],
            show['guid'],
            episode['originally_available_at']))
    db.commit()

db.close()
print('Done.')
