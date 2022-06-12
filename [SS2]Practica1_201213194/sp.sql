use dwrm;
drop procedure if exists sp_carga_canciones_dw;
DELIMITER //
CREATE PROCEDURE sp_carga_canciones_dw() 
begin
  
  DECLARE var_final INTEGER DEFAULT 0;  
  DECLARE w_cod_genero INT;
  DECLARE w_cod_artista INT;
  DECLARE w_cod_cancion INT;
  
  Declare w_artista varchar(250);
  DECLARE w_cancion VARCHAR(250);
  DECLARE w_duration_ms VARCHAR(45);
  DECLARE w_explicit VARCHAR(45);
  DECLARE w_vyear INT;
  DECLARE w_popularity DECIMAL(10,4);
  DECLARE w_danceability DECIMAL(10,4);
  DECLARE w_energy DECIMAL(10,4);
  DECLARE w_ekey INT;
  DECLARE w_loudness DECIMAL(10,4);
  DECLARE w_modo DECIMAL(10,4);
  DECLARE w_speechiness DECIMAL(10,4);
  DECLARE w_acousticness DECIMAL(12,9);
  DECLARE w_instrumentalness DECIMAL(12,9);
  DECLARE w_liveness DECIMAL(10,4);
  DECLARE w_valence DECIMAL(10,4);
  DECLARE w_tempo DECIMAL(10,4);
  DECLARE w_genero VARCHAR(150);
  DECLARE lista_canciones CURSOR FOR 
  SELECT artist,song,duration_ms,explicit,anio,popularity,danceability
		,energy,llave,loudness,modo,speechiness,acousticness,instrumentalness
		,liveness,valence,tempo,genre
  FROM tmp;  
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET var_final = 1;  
    
  OPEN lista_canciones;
  bucle: LOOP
    FETCH lista_canciones INTO 
    w_artista,w_cancion,w_duration_ms,w_explicit,w_vyear,w_popularity,w_danceability
    ,w_energy,w_ekey,w_loudness,w_modo,w_speechiness,w_acousticness,w_instrumentalness
    ,w_liveness,w_valence,w_tempo,w_genero;
    IF var_final = 1 THEN
      LEAVE bucle;
    END IF;    
    /*OBTIENE EL CÓDIGO DEL GENERO*/
    set w_cod_genero =(select id from genero where nombre = w_genero);
    /*OBTIENE EL CÓDIGO DEL ARTISTA*/
    set w_cod_artista=(select id from artista where id>0 and nombre =w_artista);    
        
    if exists(select 1 from artista art, cancion can, reproduccion_musical rep, genero gen
				where art.id=rep.artista_id
                and can.id=rep.cancion_id
                and art.nombre = w_artista
                and can.nombre=w_cancion
                and can.genero_id=gen.id
                and gen.nombre = w_genero
                and can.duration_ms=w_duration_ms
                and can.vyear=w_vyear
                and can.popularity=w_popularity) then 
                
		set w_cod_cancion = (select can.id from artista art, cancion can, reproduccion_musical rep, genero gen
				where art.id=rep.artista_id
                and can.id=rep.cancion_id
                and art.nombre = w_artista
                and can.nombre=w_cancion
                and can.genero_id=gen.id
                and gen.nombre = w_genero
                and can.duration_ms=w_duration_ms
                and can.vyear=w_vyear
                and can.popularity=w_popularity);           
		update reproduccion_musical set num_reproducciones=num_reproducciones+1 
		where artista_id=w_cod_artista and cancion_id=w_cod_cancion;		        		
	else
		INSERT INTO cancion(genero_id,duration_ms,explicit,vyear,popularity,danceability,energy,ekey
			,loudness,modo,speechiness,acousticness,instrumentalness,liveness,valence,tempo,nombre)
			values(w_cod_genero,w_duration_ms,w_explicit,w_vyear,w_popularity,w_danceability
			,w_energy,w_ekey,w_loudness,w_modo,w_speechiness,w_acousticness,w_instrumentalness
			,w_liveness,w_valence,w_tempo,w_cancion);	
            
            set w_cod_cancion = (select last_insert_id());            
            insert into reproduccion_musical(artista_id,cancion_id,num_reproducciones)  values(w_cod_artista,w_cod_cancion,1);    
	end if;
    set w_cod_cancion=null;
  END LOOP bucle;
  CLOSE lista_canciones;
end;
//
DELIMITER ;
