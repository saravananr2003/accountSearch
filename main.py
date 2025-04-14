import os
import shutil
import tempfile
import uuid

from flask import Flask, request, redirect, url_for, render_template_string

# Flask app setup
app = Flask(__name__)
app.config['BASE_FOLDER'] = '/Users/ramsa005/Desktop/Staples/Projects/Git/accountSearch/datafiles/'
app.config['INCOMING_FOLDER'] = app.config['BASE_FOLDER'] + "incoming/"  # using a temporary directory
app.config['TEMP_FOLDER'] = app.config['BASE_FOLDER'] + "temp/"
app.config['OUTPUT_FOLDER'] = app.config['BASE_FOLDER'] + "output/"  # using a temporary directory
app.config['PROCESS_FOLDER'] = app.config['BASE_FOLDER'] + "process/"
app.config['UPLOAD_FOLDER'] = tempfile.gettempdir()



# A simple upload form
UPLOAD_FORM = """
<!doctype html>
<title>Upload CSV</title>
<h1>Upload a CSV File</h1>
<form method=post action="/" enctype=multipart/form-data>
  <input type=file name=file>
  <input type=submit value=Upload>
</form>

<form method=post action="/process">
	<input type=submit value=Process>
</form>
"""



@app.route('/', methods=['GET', 'POST'])
def upload_file():
	if request.method == 'POST':
		# Check if the file part is present in request
		if 'file' not in request.files:
			return "No file part in the request", 400

		file = request.files['file']
		if file.filename == '':
			return "No selected file", 400

		message_key = str(uuid.uuid4())
		output_filename = file.filename + "." + message_key
		tmp_file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
		file.save(tmp_file_path)

		inc_file_path = os.path.join(app.config['INCOMING_FOLDER'], output_filename)
		prcs_file_path = os.path.join(app.config['PROCESS_FOLDER'], output_filename)
		message = load_file_to_incoming(tmp_file_path, inc_file_path)

		# process_file(inc_file_path, prcs_file_path)

		return message_key

		# Load the file into Snowflake
		# try:
		# 	load_file_to_snowflake(temp_file_path, file.filename)
		# 	message = "File successfully loaded to Snowflake."
		# except Exception as e:
		# 	message = f"An error occurred: {e}"
		# finally:
		# 	# Optionally, delete the temporary file
		# 	os.remove(temp_file_path)

		# return message
	return render_template_string(UPLOAD_FORM)

@app.route('/process', methods=['POST'])
def load_file_to_incoming(pv_src_file, pv_dst_file):
	# global src_file_path, dst_file_path
	try:
		# Save the file to a temporary location
		src_file_path = pv_src_file
		dst_file_path = pv_dst_file

		shutil.copy(src_file_path, dst_file_path)
		message = "File Successfully loaded into the incoming location"
	except Exception as e:
		message = f"Error occurred while uploading file {src_file_path} to {dst_file_path} : {e}"
	return message



if __name__ == '__main__':
	# Run the Flask app
	app.run(debug=True)
