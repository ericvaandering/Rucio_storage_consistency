FROM cmssw/rucio-daemons:release-1.25.2.nano1
USER root

RUN mkdir -p /deckard
# COPY vomses /etc
# COPY run.sh site.sh /consistency/
COPY deckard_run.sh /deckard/
WORKDIR /deckard

# RUN git clone https://github.com/ivmfnal/cms_consistency.git

WORKDIR /deckard
COPY deckard_run.sh .

CMD /bin/bash


