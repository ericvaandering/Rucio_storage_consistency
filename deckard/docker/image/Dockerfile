FROM cmssw/rucio-daemons:release-1.25.2.nano1
USER root

RUN mkdir -p /deckard
COPY deckard_run.sh /deckard/
WORKDIR /deckard

RUN git clone https://github.com/piperov/Rucio_storage_consistency.git
RUN git clone https://github.com/ivmfnal/cms_consistency.git

COPY deckard_run.sh .
RUN chmod +x deckard_run.sh

CMD /bin/bash


