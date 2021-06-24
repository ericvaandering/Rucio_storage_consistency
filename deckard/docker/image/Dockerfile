FROM cmssw/rucio-daemons:release-1.25.2.nano1
USER root

# Python packages
RUN pip3  --no-cache-dir install pyyaml

# jobber
RUN rpm -i https://github.com/dshearer/jobber/releases/download/v1.4.0/jobber-1.4.0-1.el7.x86_64.rpm
RUN mkdir -p /var/log/deckard-jobber-successes /var/log/deckard-jobber-errors /var/log/deckard-jobber-failures

RUN mkdir -p /deckard
COPY deckard_run.sh /deckard/
COPY deckard_site.sh /deckard/
COPY deckard_jobber.yaml /deckard/
WORKDIR /deckard

RUN git clone https://github.com/piperov/Rucio_storage_consistency.git
RUN git clone https://github.com/ivmfnal/cms_consistency.git

RUN chmod +x deckard_run.sh
RUN chmod +x deckard_site.sh

CMD /bin/bash


