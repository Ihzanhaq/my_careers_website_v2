from flask import Flask, render_template,jsonify

app=Flask(__name__)

JOBS=[
  {
    'id':1,
    'title':'Data Analyst',
    'location':'Trivandrum, India',
    'salary':'Rs. 10,00,000'
  },
  {
     'id':2,
     'title':'Flutter Developer',
     'location':'Ernakulam, India',
     'salary':'Rs. 15,00,000'
  },
  {
    'id':3,
    'title':'Full Stack Developer',
    'location':'Banglore, India',
    'salary':'Rs. 20,00,000'
  },
  {
    'id':4,
    'title':'Software Engineer',
    'location':'Mumbai, India',
    
  },
]
@app.route("/")
def hello():
  return render_template("home.html",jobs=JOBS)

@app.route("/api/jobs")
def list_jobs():
  return jsonify(JOBS)

if( __name__ == "__main__"):
  app.run(host="0.0.0.0",debug=True)