# To build an autoencoder, you need three things:
# encoding function
# decoding function
# distance function between the amount of information loss between the compressed representation of your data and the decompressed representation (i.e. a "loss" function).
# https://blog.keras.io/building-autoencoders-in-keras.html

# USE: https://towardsdatascience.com/implementing-a-simple-auto-encoder-in-tensorflow-1181751f202
# https://github.com/aymericdamien/TensorFlow-Examples/blob/master/examples/3_NeuralNetworks/autoencoder.py

import tensorflow as tf
from keras.layers import Input, Dense
from keras.models import Model, Sequential

class autoencoder:
    def __init__(self, n_input, n_h1, n_h2, n_latent, learning_rate=0.01, n_batch=100, n_epoch=100):

        self.n_input = n_input
        self.n_h1 = n_h1
        self.n_h2 = n_h2
        self.n_latent = n_latent

        self.learning_rate = learning_rate
        self.n_batch = n_batch
        self.n_epoch = n_epoch

        self.autoencoder = self.create_autoencoder()

    def create_autoencoder(self):
        autoencoder = Sequential()
        autoencoder.add(Dense(self.n_h1, input_shape=(self.n_input,), activation='relu', kernel_initializer='glorot_uniform'))
        autoencoder.add(Dense(self.n_h2, activation='relu', kernel_initializer='glorot_uniform'))
        autoencoder.add(Dense(self.n_latent, activation='relu', kernel_initializer='glorot_uniform'))
        autoencoder.add(Dense(self.n_h2, activation='relu', kernel_initializer='glorot_uniform'))
        autoencoder.add(Dense(self.n_h1, activation='relu', kernel_initializer='glorot_uniform'))
        autoencoder.add(Dense(self.n_input, kernel_initializer='glorot_uniform'))
        autoencoder.compile(optimizer=tf.compat.v1.train.AdamOptimizer(self.learning_rate),loss=tf.compat.v1.losses.mean_squared_error)
        autoencoder.summary()
        return autoencoder

    def encode(self):
        return None

    def decode(self):
        return None

    def train(self):
        return None

if __name__ == '__main__':
    test = autoencoder(5,5,5,5)