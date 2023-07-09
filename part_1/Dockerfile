FROM python:3.8-buster

COPY /iibflix /iibflix

RUN curl https://files.grouplens.org/datasets/movielens/ml-25m.zip -o /ml-25m.zip \
&& unzip /ml-25m.zip \
&& rm /ml-25m.zip \
&& mv /ml-25m/* /iibflix \
&& rm -r /ml-25m

WORKDIR /iibflix

RUN pip install pandas click
RUN pip install .

EXPOSE 8080

CMD ["/bin/bash"]

