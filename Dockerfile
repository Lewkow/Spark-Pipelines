FROM jupyter/all-spark-notebook:latest

RUN pip install scipy==1.2.1
RUN conda install -c https://conda.anaconda.org/amueller wordcloud

