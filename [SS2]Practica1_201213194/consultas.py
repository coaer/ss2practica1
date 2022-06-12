QUERY1 =('select art.nombre,sum(mus.num_reproducciones) as reproducciones'
    'from artista art, reproduccion_musical mus, cancion can'
    'where art.id=mus.artista_id'
    'and mus.cancion_id=can.id'
    'group by nombre'
    'order by reproducciones '
    'desc limit 10')