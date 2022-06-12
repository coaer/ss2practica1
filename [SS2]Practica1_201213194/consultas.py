QUERY1 =('select art.nombre,sum(mus.num_reproducciones) as reproducciones '
    'from artista art, reproduccion_musical mus, cancion can '
    'where art.id=mus.artista_id '
    'and mus.cancion_id=can.id '
    'group by nombre '
    'order by reproducciones '
    'desc limit 10')



QUERY2=('select art.nombre, can.nombre,sum(mus.num_reproducciones) as reproducciones '
    'from cancion can, reproduccion_musical mus, artista art '
    'where can.id=mus.cancion_id '
    'and mus.artista_id=art.id '
    'group by art.nombre, can.nombre '
    'order by reproducciones '
    'desc limit 10;')

QUERY3=('select gen.nombre,sum(mus.num_reproducciones) as reproducciones '
    'from cancion can, reproduccion_musical mus, artista art,genero gen '
    'where can.id=mus.cancion_id '
    'and mus.artista_id=art.id '
    'and can.genero_id=gen.id '
    'group by gen.nombre '
    'order by reproducciones '
    'desc limit 5;')

QUERY4 = ('select resultado.nom as Artista , resultado.gen as Genero , max(resultado.reproducciones)  as Total_Reproducciones from (select '
	    'art.nombre as nom '
        ', gen.nombre as gen '
	    ',sum(mus.num_reproducciones) as reproducciones '
        'from cancion can, reproduccion_musical mus, artista art,genero gen '
        'where can.id=mus.cancion_id '
        'and mus.artista_id=art.id '
        'and can.genero_id=gen.id '
        'group by art.nombre, gen.nombre '
        'order by reproducciones '
        'desc) resultado '
        'where resultado.reproducciones>=0 '
        'group by resultado.gen '
        'order by reproducciones desc;')

QUERY5=('select resultado.nom as Cancion ,resultado.arti as Artista, resultado.gener as Genero , max(resultado.reproducciones)  as Total_Reproducciones '
    'from (select can.nombre as nom,gen.nombre as gener,art.nombre as arti,sum(mus.num_reproducciones) as reproducciones '
    'from cancion can, reproduccion_musical mus, artista art,genero gen '
    'where can.id=mus.cancion_id '
    'and mus.artista_id=art.id '
    'and can.genero_id=gen.id '
    'group by can.nombre, gen.nombre,art.nombre '
    'order by reproducciones '
    'desc) resultado '
    'where resultado.reproducciones>=0 '
    'group by resultado.gener '
    'order by reproducciones desc;')


QUERY6=('select resultado.nom as Cancion ,resultado.arti as Artista, resultado.gen as Genero , max(resultado.reproducciones)  as Total_Reproducciones,resultado.anio as anio '
    'from (select '
        'can.nombre as nom '
        ', gen.nombre as gen '
        ',art.nombre as arti '
        ',can.vyear as anio '
        ',sum(mus.num_reproducciones) as reproducciones '
    'from cancion can, reproduccion_musical mus, artista art,genero gen '
    'where can.id=mus.cancion_id '
    'and mus.artista_id=art.id '
    'and can.genero_id=gen.id '
    'group by can.nombre, gen.nombre,art.nombre,can.vyear '
    'order by reproducciones  '
    'desc) resultado '
    'where resultado.reproducciones>=0 '
    'group by resultado.anio '
    'order by resultado.anio asc;') 


QUERY7=('select	'
        'art.nombre as artista '
        ',sum(can.popularity) as popularidad '
    'from cancion can, reproduccion_musical mus, artista art '
    'where can.id=mus.cancion_id '
    'and mus.artista_id=art.id '
    'group by art.nombre '
    'order by popularidad  '
    'desc limit 10;')

QUERY8=('select distinct art.nombre, can.nombre,max(can.popularity) as popularidad '
    'from cancion can, reproduccion_musical mus, artista art '
    'where can.id=mus.cancion_id '
    'and mus.artista_id=art.id '
    'group by art.nombre, can.nombre '
    'order by popularidad  '
    'desc limit 10;')

QUERY9=('select '
        'gen.nombre as GENERO '
        ',sum(can.popularity*rep.num_reproducciones) as popularidad '
    'from cancion can,genero gen, reproduccion_musical rep '
    'where can.id>0 '
    'and can.genero_id=gen.id '
    'and rep.cancion_id=can.id '
    'group by gen.nombre '
    'order by popularidad '
    'desc limit 5;')


QUERY10=('select resultado.nom as Cancion ,resultado.arti as Artista, resultado.gen as Género from (select '
        'can.nombre as nom'
        ', gen.nombre as gen '
        ',art.nombre as arti '
    'from cancion can, reproduccion_musical mus, artista art,genero gen '
    'where can.id=mus.cancion_id '
    'and mus.artista_id=art.id '
    'and can.genero_id=gen.id '
    'and can.explicit="TRUE" '
    'group by can.nombre, gen.nombre,art.nombre '
    'order by gen.nombre  '
    'asc) resultado '
    'where resultado.nom<>"" '
    'group by resultado.gen;')