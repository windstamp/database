# encoding=utf-8

import logging

logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                filename='combination.log',
                filemode='w')
				
console = logging.StreamHandler()
# console.setLevel(logging.DEBUG)
console.setLevel(logging.INFO)
#formatter = logging.Formatter('%(asctime)-s: %(filename)s[line:%(lineno)d] %(levelname)-s %(message)s')
formatter = logging.Formatter('%(levelname)-s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

def check_combination(cursor_db_src_1, cursor_db_src_2, cursor_dst, tbl_name):	
    counts_db_src_1 = get_tbl_counts(cursor_db_src_1, tbl_name)
    counts_db_src_2 = get_tbl_counts(cursor_db_src_2, tbl_name)
    counts_dst = get_tbl_counts(cursor_dst, tbl_name)
	
    logging.debug( 'counts_db_src_1 = %s' % (counts_db_src_1) )
    logging.debug( 'counts_db_src_2 = %s' % (counts_db_src_2) )
    logging.debug( 'counts_dst = %s' % (counts_dst) )
	
    if counts_db_src_1 + counts_db_src_2 != counts_dst:
        return False, counts_db_src_1, counts_db_src_2, counts_dst
		
    return True, counts_db_src_1, counts_db_src_2, counts_dst

def check_tbl_after_combination(cursor_db_src_1, cursor_db_src_2, cursor_db_dst, tbl_name):
    logging.info( 'check table %s start...' % (tbl_name) )
    
    checked, counts_db_src_1, counts_db_src_2, counts_dst = check_combination(cursor_db_src_1, cursor_db_src_2, cursor_db_dst, tbl_name)
    
    if not checked:
        logging.error( 'combination table %s end failed (%d + %d != %d)!!!' % (tbl_name, counts_db_src_1, counts_db_src_2, counts_dst) )
    else:
        logging.info( 'combination table %s end successfully (%d + %d = %d)!!!' % (tbl_name, counts_db_src_1, counts_db_src_2, counts_dst) )
	
	
    if (not checked):
        logging.error( 'check table %s end failed!!!' % (tbl_name) )
		
        return False
	
    logging.info( 'check table %s end successfully!!!' % (tbl_name) )
    return True

def check_tbl_before_combination(cursor_db_src_1, cursor_db_src_2, cursor_db_dst, conn_db_dst, tbl_name):
    logging.info( 'check table %s start...' % (tbl_name) )
    
    fields_db_src_1 = get_tbl_fields(cursor_db_src_1, tbl_name)
    logging.debug( 'fields_db_src_1 = %s' % ( ", ".join(fields_db_src_1) ) )
    
    fields_db_src_2 = get_tbl_fields(cursor_db_src_2, tbl_name)
    logging.debug( 'fields_db_src_2 = %s' % ( ", ".join(fields_db_src_2) ) )
    
    checked = check_tbl_by_fields(fields_db_src_1, fields_db_src_2)
    if (not checked):
        logging.error( 'check table %s end failed!!!' % (tbl_name) )
		
        return False
	
    logging.info( 'check table %s end successfully!!!' % (tbl_name) )
    return True

def check_tbl_by_fields(fields1, fields2):
    logging.debug( type(fields1) )
    logging.debug( type(fields2) )
	
    if ( ( not isinstance(fields1, list) ) and ( not isinstance(fields1, tuple) ) ):
        return False
	
    if ( ( not isinstance(fields2, list) ) and ( not isinstance(fields2, tuple) ) ):
        return False
	
    if (fields1 != fields2):
        return False
	
    return True

def combinate_tbl_by_name(cursor_db_src_1, cursor_db_src_2, cursor_db_dst, conn_db_dst, tbl_name, check):
    logging.info( 'combination table %s start...' % (tbl_name) )
    
    fields_db_src_1 = get_tbl_fields(cursor_db_src_1, tbl_name)
    logging.debug( 'fields_db_src_1 = %s' % ( ", ".join(fields_db_src_1) ) )
    
    fields_db_src_2 = get_tbl_fields(cursor_db_src_2, tbl_name)
    logging.debug( 'fields_db_src_2 = %s' % ( ", ".join(fields_db_src_2) ) )
    
    checked = check_tbl_by_fields(fields_db_src_1, fields_db_src_2)
    if (not checked):
        logging.error( 'Fields of table %s check failed!!!' % (tbl_name) )
    
    combinate_tbl_by_fields(cursor_db_src_1, cursor_db_dst, tbl_name, fields_db_src_1)
    combinate_tbl_by_fields(cursor_db_src_2, cursor_db_dst, tbl_name, fields_db_src_2)
    conn_db_dst.commit()
	
    if not check:
        logging.error( 'combination table %s end' % (tbl_name) )
    
    if check:
        checked, counts_db_src_1, counts_db_src_2, counts_dst = check_combination(cursor_db_src_1, cursor_db_src_2, cursor_db_dst, tbl_name)
        
        if not checked:
            logging.error( 'combination table %s end failed (%d + %d != %d)!!!' % (tbl_name, counts_db_src_1, counts_db_src_2, counts_dst) )
        else:
            logging.info( 'combination table %s end successfully (%d + %d = %d)!!!' % (tbl_name, counts_db_src_1, counts_db_src_2, counts_dst) )
	
def combinate_tbl_by_fields(cursor_src, cursor_dst, tbl_name, fields):
    sqlQuery = "SELECT " + ", ".join(fields) + " FROM " + tbl_name + ";"
    logging.debug( 'sqlQuery = %s' % (sqlQuery) )
	
    points = list()
    for i in range( len(fields) ):
        points.append('%s')	
    sqlInsert = "INSERT INTO  " + tbl_name + " (" + ", ".join(fields) + ") VALUES (" + ", ".join(points) + ")"
    logging.debug( 'sqlInsert = %s' % (sqlInsert) )
	
    cursor_src.execute(sqlQuery)
    rows = cursor_src.fetchall()
    for row in rows:
        logging.debug('type(row) = %s' % (type(row)))
        logging.debug('row = %s' % (", ".join(row)))
        cursor_dst.execute(sqlInsert, row)

def get_tbl_counts(cursor, tbl_name):
    cursor.execute("SELECT COUNT(*) FROM " + tbl_name)
    rows = cursor.fetchall()
    counts = rows[0][0]
    logging.debug('type(counts) = %s' % (type(counts)))
    logging.debug('counts = %s' % (counts))
    
    return counts
	
def get_tbl_fields(cursor, tbl_name):
    fields = list()
    
    cursor.execute('SHOW COLUMNS FROM ' + tbl_name)
    rows = cursor.fetchall()
    
    for row in rows:
        fields.append( row[0] )
    
    fields.sort()
	
    return fields