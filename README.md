# Web_Search_Engine

This project is a course assignment for the course `COL764 - Information Retrieval and Web Search` at IIT Delhi.

The project aims at building a web search engine with implementations of `multiple encoding and decoding` techniques which help to compress the huge pool of data the search engine stores.

The data is initially stored as self-constructed `inverse-index dictionaries`.

To handle the huge amount of data which cannot be stored simultaneously on the in-memory disk, we make use of self-implemented `External Merge Sort` which helps to minimize use of in-memory disk thereby boosting the retrival speed of our engine.

The entire engine is implemented in `Python`.
