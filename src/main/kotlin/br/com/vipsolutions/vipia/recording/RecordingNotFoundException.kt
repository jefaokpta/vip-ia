package br.com.vipsolutions.vipia.recording

class RecordingNotFoundException(val key: String) : RuntimeException("Gravacao nao encontrada no storage: $key")
