import boto3
from flask import Flask, request, jsonify
import uuid
from datetime import datetime, timedelta
import schedule
import time
import threading


ALLOWED_TYPES = ['image/jpeg', 'image/png', 'image/gif', 'application/pdf']

app = Flask(__name__)



def archivo_es_permitido(file):
    file_type = file.mimetype
    return file_type in ALLOWED_TYPES

def upload_file_to_s3(file, file_name, task_id):
    try:
        s3.upload_fileobj(
            file,
            BUCKET_NAME,
            file_name,
            ExtraArgs={
                'Metadata': {
                    'tarea': task_id,
                    'cantidadDescargas': '0' 
                }
            }
        )
        s3_url = f"https://{BUCKET_NAME}.s3.amazonaws.com/{file_name}"
        return s3_url
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return None
    
@app.route('/tareas', methods=['POST'])
def create_task():
    try:

        data = request.form
        title = data.get('Titulo')
        description = data.get('Descripcion', '')
        due_date = data.get('fecha_de_entrega')

        task_id = str(uuid.uuid4())
        attachments = []
        for i in range(3): 
            file = request.files.get(f'attachment{i+1}')
            if file:
                if not archivo_es_permitido(file):
                    return jsonify({'error': f"Invalid file type: {file.mimetype}. Only images and PDFs are allowed."}), 400
                
                file_name = f"{str(uuid.uuid4())}_{file.filename}"
                s3_url = upload_file_to_s3(file, file_name, task_id)
                if s3_url:
                    attachments.append(s3_url)


        
        created_at = datetime.utcnow().isoformat()
        task = {
            'Tareaid': task_id,
            'Titulo': title,
            'Descripcion': description,
            'fechacreacion': created_at,
            'archivos': attachments, 
            'fecha_de_entrega': due_date
        }


        table.put_item(Item=task)

        message = f'Se ha creado una nueva tarea'
        sns.publish(TopicArn=TOPIC_ARN,Message=message)

        return jsonify({'message': 'Task created successfully', 'task': task}), 201

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/tareas', methods=['GET'])
def get_tareas():
    try:

        response = table.scan(ProjectionExpression="Tareaid, Titulo, Descripcion, fecha_de_entrega, fechacreacion")
        tareas = response.get('Items', [])
        
        return jsonify({'tareas': tareas}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/tareas/<Tareaid>', methods=['GET'])
def get_task(Tareaid):
    try:
        response = table.get_item(Key={'Tareaid': Tareaid})
        
        if 'Item' in response:
            task = response['Item']
            attachments = task.get('archivos', []) 

            
            for attachment_url in attachments:
                s3_key = attachment_url.replace(f"https://{BUCKET_NAME}.s3.amazonaws.com/", "")
                
               
                try:
                    head_object = s3.head_object(Bucket=BUCKET_NAME, Key=s3_key)
                    current_download_count = int(head_object['Metadata'].get('x-amz-meta-cantidaddescargas', '0'))

                    new_download_count = current_download_count + 1
           
                    s3.copy_object(
                        Bucket=BUCKET_NAME,
                        CopySource={'Bucket': BUCKET_NAME, 'Key': s3_key},
                        Key=s3_key,
                        Metadata={
                            'x-amz-meta-tarea': Tareaid,
                            'x-amz-meta-cantidaddescargas': str(new_download_count)
                        },
                        MetadataDirective='REPLACE'
                    )
                except Exception as e:
                    print(f"Error updating download count for {s3_key}: {e}")

            return jsonify({'tarea': task}), 200
        else:
            return jsonify({'message': 'Task not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/tareas/<Tareaid>', methods=['DELETE'])
def eliminar_tarea(Tareaid):
    try:
        response = table.get_item(Key={'Tareaid': Tareaid})
        
        if 'Item' in response:
            task = response['Item']
            attachments = task.get('archivos', [])


            for attachment_url in attachments:

                s3_key = attachment_url.replace(f"https://{BUCKET_NAME}.s3.amazonaws.com/", "")
                try:
                    s3.delete_object(Bucket=BUCKET_NAME, Key=s3_key)
                except Exception as s3_error:
                    print(f"Error deleting file {s3_key}: {s3_error}")

            table.delete_item(Key={'Tareaid': Tareaid})

            message = f'Se ha eliminado la tarea con id: {Tareaid}'
            sns.publish(TopicArn=TOPIC_ARN,Message=message)
            return jsonify({'message': 'Tarea eliminada exitosamente'}), 200
        else:
            return jsonify({'message': 'No se pudo eliminar una tarea con ese id'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/tareas/<Tareaid>', methods=['PUT'])
def update_task(Tareaid):
    try:
 
        response = table.get_item(Key={'Tareaid': Tareaid})
        
        if 'Item' in response:
            task = response['Item']
            old_attachments = task.get('archivos', [])  

           
            data = request.form
            title = data.get('Titulo', task.get('Titulo'))
            description = data.get('Descripcion', task.get('Descripcion'))
            due_date = data.get('fecha_de_entrega', task.get('fecha_de_entrega'))

            new_attachments = []

            for i in range(3):  
                file = request.files.get(f'attachment{i+1}')
                
                if file:
                    if not archivo_es_permitido(file):
                        return jsonify({'error': f"Invalid file type: {file.mimetype}. Only images and PDFs are allowed."}), 400
                    
                    file_name = f"{str(uuid.uuid4())}_{file.filename}"

                    
                    existing_file_url = next((url for url in old_attachments if file.filename in url), None)
                    if existing_file_url:
                  
                        s3_key = existing_file_url.replace(f"https://{BUCKET_NAME}.s3.amazonaws.com/", "")
                        s3.delete_object(Bucket=BUCKET_NAME, Key=s3_key)  

                 
                    s3_url = upload_file_to_s3(file, file_name, Tareaid)
                    if s3_url:
                        new_attachments.append(s3_url)

           
            if not new_attachments and old_attachments:
                new_attachments = old_attachments
            else:
              
                for old_attachment in old_attachments:
                    if old_attachment not in new_attachments:
                        s3_key = old_attachment.replace(f"https://{BUCKET_NAME}.s3.amazonaws.com/", "")
                        s3.delete_object(Bucket=BUCKET_NAME, Key=s3_key)


            updated_task = {
                'Tareaid': Tareaid,
                'Titulo': title,
                'Descripcion': description,
                'fechacreacion': task.get('fechacreacion'), 
                'archivos': new_attachments,
                'fecha_de_entrega': due_date
            }


            table.put_item(Item=updated_task)

            message = f'Se ha actualizado la tarea con id: {Tareaid}'
            sns.publish(TopicArn=TOPIC_ARN,Message=message)

            return jsonify({'message': 'Task updated successfully', 'task': updated_task}), 200
        else:
            return jsonify({'message': 'Task not found'}), 404

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    


def delete_old_tasks():
    threshold_date = datetime.utcnow() - timedelta(days=30)

    response = table.scan()
    old_tasks = []

    for item in response.get('Items', []):
        creation_date = item.get('fechacreacion')
        if creation_date:
            creation_date = datetime.fromisoformat(creation_date) 
            if creation_date < threshold_date:
                old_tasks.append(item)

    for task in old_tasks:
        task_id = task['Tareaid']
        attachments = task.get('archivos', [])

        for attachment in attachments:
            s3_key = attachment.replace(f"https://{BUCKET_NAME}.s3.amazonaws.com/", "")
            s3.delete_object(Bucket=BUCKET_NAME, Key=s3_key)

        table.delete_item(Key={'Tareaid': task_id})
        print(f"Deleted task {task_id} and its attachments.")

def run_scheduler():
    schedule.every().day.at("00:00").do(delete_old_tasks)

    while True:
        schedule.run_pending()
        time.sleep(60)  

if __name__ == '__main__':

    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.start()

    #delete_old_tasks()
    app.run(debug=True)