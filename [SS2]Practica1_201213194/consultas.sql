use dwrm;
/*REALIZA CONSULTAS*/
/*CONSULTA 1: TOP 10 DE ARTISTAS CON MAYORES REPRODUCCIONES*/
select art.nombre,sum(mus.num_reproducciones) as reproducciones
from artista art, reproduccion_musical mus, cancion can
where art.id=mus.artista_id
and mus.cancion_id=can.id
group by nombre
order by reproducciones 
desc limit 10;


/*CONSULTA 2: TOP 10 DE CANCIONES CON MAYORES REPRODUCCIONES*/
select art.nombre, can.nombre,sum(mus.num_reproducciones) as reproducciones
from cancion can, reproduccion_musical mus, artista art
where can.id=mus.cancion_id
and mus.artista_id=art.id
group by art.nombre, can.nombre
order by reproducciones 
desc limit 10;



/*CONSULTA 3: TOP 5 GENEROS MÁS REPRODUCIDOS*/
select gen.nombre,sum(mus.num_reproducciones) as reproducciones
from cancion can, reproduccion_musical mus, artista art,genero gen
where can.id=mus.cancion_id
and mus.artista_id=art.id
and can.genero_id=gen.id
group by gen.nombre
order by reproducciones 
desc limit 5;


/*CONSULTA 4: EL ARTISTA MÁS REPRODUCIDO POR GENERO*/
select resultado.nom as 'Artista' , resultado.gen as 'Género' , max(resultado.reproducciones)  as 'Total Reproducciones' from (select 
	art.nombre as nom
    , gen.nombre as gen
	,sum(mus.num_reproducciones) as reproducciones
from cancion can, reproduccion_musical mus, artista art,genero gen
where can.id=mus.cancion_id
and mus.artista_id=art.id
and can.genero_id=gen.id
group by art.nombre, gen.nombre
order by reproducciones 
desc) resultado
where resultado.reproducciones>=0
group by resultado.gen
order by reproducciones desc;


/*CONSULTA 5: CANCION MÁS REPRODUCIDA POR GENERO*/
select resultado.nom as 'Canción' ,resultado.arti as 'Artista', resultado.gen as 'Género' , max(resultado.reproducciones)  as 'Total Reproducciones' from (select 
	can.nombre as nom
    , gen.nombre as gen
    ,art.nombre as arti
	,sum(mus.num_reproducciones) as reproducciones
from cancion can, reproduccion_musical mus, artista art,genero gen
where can.id=mus.cancion_id
and mus.artista_id=art.id
and can.genero_id=gen.id
group by can.nombre, gen.nombre,art.nombre
order by reproducciones 
desc) resultado
where resultado.reproducciones>=0
group by resultado.gen
order by reproducciones desc;



/*CONSULTA 6: CANCION MÁS REPRODUCIDA POR CADA AÑO QUE FUE LANZADA*/
select resultado.nom as 'Canción' ,resultado.arti as 'Artista', resultado.gen as 'Género' , max(resultado.reproducciones)  as 'Total Reproducciones',resultado.anio as 'anio'
 from (select 
	can.nombre as nom
    , gen.nombre as gen
    ,art.nombre as arti
    ,can.vyear as anio
	,sum(mus.num_reproducciones) as reproducciones
from cancion can, reproduccion_musical mus, artista art,genero gen
where can.id=mus.cancion_id
and mus.artista_id=art.id
and can.genero_id=gen.id
group by can.nombre, gen.nombre,art.nombre,can.vyear
order by reproducciones 
desc) resultado
where resultado.reproducciones>=0
group by resultado.anio
order by resultado.anio asc;



/*CONSULTA 7: 10 ARTISTAS MÁS POPULARES*/
select 	
	art.nombre as artista
	,sum(can.popularity) as popularidad
from cancion can, reproduccion_musical mus, artista art
where can.id=mus.cancion_id
and mus.artista_id=art.id
group by art.nombre
order by popularidad 
desc;

/*CONSULTA 8: 10 CANCIONES MÁS POPULARES*/
select distinct art.nombre, can.nombre,max(can.popularity) as popularidad
from cancion can, reproduccion_musical mus, artista art
where can.id=mus.cancion_id
and mus.artista_id=art.id
group by art.nombre, can.nombre
order by popularidad 
desc limit 10;


/*CONSULTA 9: 5 Generos más populares*/
select 
	gen.nombre as 'GÉNERO'
	,sum(can.popularity*rep.num_reproducciones) as popularidad
from cancion can,genero gen, reproduccion_musical rep
where can.id>0
and can.genero_id=gen.id
and rep.cancion_id=can.id
group by gen.nombre
order by popularidad
desc limit 5;


/*CONSULTA 10: La canción más explicita por género.*/
select resultado.nom as 'Canción' ,resultado.arti as 'Artista', resultado.gen as 'Género' from (select 
	can.nombre as nom
    , gen.nombre as gen
    ,art.nombre as arti	
from cancion can, reproduccion_musical mus, artista art,genero gen
where can.id=mus.cancion_id
and mus.artista_id=art.id
and can.genero_id=gen.id
and can.explicit='TRUE'
group by can.nombre, gen.nombre,art.nombre
order by gen.nombre 
asc) resultado
where resultado.nom<>''
group by resultado.gen;