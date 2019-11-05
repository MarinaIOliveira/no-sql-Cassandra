#Codigo para realizar a conecxão com o servidor do banco cassandra
from cassandra.cluster import Cluster
cluster = Cluster()
#Cria uma conecxão com o keyspace Exercicio
sesseion = cluster.connect('exercicio')

#Criando uma função para imprimir os resultados das consultas
#Imprime o resultado da ação anterior na tela
def imprime(var):
    var2 = 'SELECT * FROM '
    rows = sesseion.execute(var2 + var)

    for row in rows:
        print row

#Inserindo a tabela Musicas 

sesseion.execute("""CREATE TABLE musicas(id uuid PRIMARY KEY,nome text,album text,artista text );""")

#Inserindo musicas na tabela  Musicas
sesseion.execute("""INSERT INTO musicas (id,nome,album,artista) VALUES (uuid(), 'Help', 'Help', 'Beatles');""")
sesseion.execute("""INSERT INTO musicas (id,nome,album,artista) VALUES (uuid(), 'Yesterday', 'Help!', 'Beatles');""")
sesseion.execute("""INSERT INTO musicas (id,nome,album,artista) VALUES (uuid(), 'Something', 'AbbeyRoad', 'Beatles');""")
sesseion.execute("""INSERT INTO musicas (id,nome,album,artista) VALUES (uuid(), 'Blackbird', 'The Beatles', 'Beatles');""")

#Criando um Indice na tabela Musicas
sesseion.execute("""CREATE INDEX ON musicas (artista);""")

#Criando a PlayList
sesseion.execute("""CREATE TABLE playlist_atual(id_playlist int,posicao int,id_musica uuid,nome text,album text,artista text,PRIMARY KEY (id_playlist, posicao));""")

#Adicionando musicas na  playlists
sesseion.execute("""INSERT INTO playlist_atual(id_playlist, posicao, id_musica, nome, album, artista)VALUES(1, 1,76900fa0-280c-4cda-b059-b519b17304fc, 'Help!', 'Help!', 'Beatles');""")
sesseion.execute("""INSERT INTO playlist_atual(id_playlist, posicao, id_musica, nome, album, artista)VALUES(1, 2,903f7e19-57b5-4bcc-98d5-6ecf547db8a9, 'Yesterday', 'Help!', 'Beatles');""")
sesseion.execute("""INSERT INTO playlist_atual(id_playlist, posicao, id_musica, nome, album, artista)VALUES(1, 3,a6944f0e-7dc4-44e8-9ba9-1e057b7d5360, 'Something', 'AbbeyRoad', 'Beatles');""")
sesseion.execute("""INSERT INTO playlist_atual(id_playlist, posicao, id_musica, nome, album, artista)VALUES(1, 4,c746805d-5239-4d6f-b024-c81f0e8c8212, 'Blackbird', 'The Beatles', 'Beatles');""")

#Criando a playlist versionada
sesseion.execute("""CREATE TABLE playlist_versionada(id_playlist int,versao int,modificacao text,PRIMARY KEY (id_playlist, versao)) WITH COMPACT STORAGE;""")

#Criando a lista de Versões 
sesseion.execute("""INSERT INTO playlist_versionada(id_playlist, versao, modificacao)VALUES(1, 1, 'ADI(Help!)');""")
sesseion.execute("""INSERT INTO playlist_versionada(id_playlist, versao, modificacao)VALUES(1, 2, 'ADI(Yesterday)');""")
sesseion.execute("""INSERT INTO playlist_versionada(id_playlist, versao, modificacao)VALUES(1, 3, 'ADI(Blackbird)');""")
sesseion.execute("""INSERT INTO playlist_versionada(id_playlist, versao, modificacao)VALUES(1, 4, 'ADI(Something)');""")
sesseion.execute("""INSERT INTO playlist_versionada(id_playlist, versao, modificacao)VALUES(1, 5, 'TROCA(3,2)');""")

imprimePlaylist('playlist_versionada')

sesseion.execute("""CREATE TABLE playlist(id int PRIMARY KEY,nome text );""")

sesseion.execute("""INSERT INTO playlist (id, nome) VALUES (1, 'Beatles forever');""")

imprimePlaylist('playlist')