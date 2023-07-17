#!/bin/bash

# Obtén todas las ramas del repositorio
branches=$(git branch --format='%(refname:short)')

# Recorre cada rama y realiza un push
for branch in $branches
do
  git push sparql2flink-hdt $branch
done

