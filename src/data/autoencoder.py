# To build an autoencoder, you need three things:
# encoding function
# decoding function
# distance function between the amount of information loss between the compressed representation of your data and the decompressed representation (i.e. a "loss" function).

import os

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
from tensorflow.keras import layers, Sequential, optimizers
from tensorflow.keras.models import load_model
from tensorflow.keras.callbacks import ModelCheckpoint

from keras.datasets import mnist
import numpy as np

(x_train, _), (x_test, _) = mnist.load_data()

class autoencoder:
    def __init__(self, n_input, n_h1, n_h2, n_latent, learning_rate=0.001, n_batch=100,
                 n_epoch=100, checkpoint_path='checkpoints/', model_name='model.h5'):

        self.n_input = n_input
        self.n_h1 = n_h1
        self.n_h2 = n_h2
        self.n_latent = n_latent

        self.learning_rate = learning_rate
        self.n_batch = n_batch
        self.n_epoch = n_epoch

        self.checkpoint = checkpoint_path
        self.model_name = model_name
        if os.path.isfile(checkpoint_path + model_name):
            self.autoencoder = self.load_autoencoder()
        else:
            self.autoencoder = self.build_autoencoder()

    def load_autoencoder(self):
        return load_model(self.checkpoint + self.model_name)

    def build_autoencoder(self):
        autoencoder = Sequential()
        autoencoder.add(layers.Dense(self.n_h1, input_shape=(self.n_input,), activation='relu',
                                           kernel_initializer='glorot_uniform', name='encoder_l1'))
        autoencoder.add(layers.Dense(self.n_h2, activation='relu', kernel_initializer='glorot_uniform',
                                           name='encoder_l2'))
        autoencoder.add(layers.Dense(self.n_latent, activation='relu', kernel_initializer='glorot_uniform',
                                           name='latent_layer'))
        autoencoder.add(layers.Dense(self.n_h2, activation='relu', kernel_initializer='glorot_uniform', name='decoder_l1'))
        autoencoder.add(layers.Dense(self.n_h1, activation='relu', kernel_initializer='glorot_uniform', name='decoder_l2'))
        autoencoder.add(layers.Dense(self.n_input, kernel_initializer='glorot_uniform', name='decoder_output'))

        optimizer = optimizers.RMSprop(self.learning_rate)
        autoencoder.compile(loss='mse', optimizer=optimizer)

        autoencoder.summary()
        return autoencoder

    def encode(self, input):
        trained_autoencoder = self.load_autoencoder()
        encoder = Sequential()
        encoder.add(trained_autoencoder.get_layer(name="encoder_l1"))
        encoder.add(trained_autoencoder.get_layer(name="encoder_l2"))
        encoder.add(trained_autoencoder.get_layer(name="latent_layer"))
        return encoder.predict(input)

    def decode(self, encoding):
        trained_autoencoder = self.load_autoencoder()
        decoder = Sequential()
        decoder.add(trained_autoencoder.get_layer(name="decoder_l1"))
        decoder.add(trained_autoencoder.get_layer(name="decoder_l2"))
        decoder.add(trained_autoencoder.get_layer(name="decoder_output"))
        return decoder.predict(encoding)

    def train(self, train_data, test_data):
        checkpoint = ModelCheckpoint(self.checkpoint + self.model_name, monitor='loss', verbose=1, save_best_only=True, mode='min')
        self.autoencoder.fit(train_data, train_data,
                             epochs=self.n_epoch,
                             batch_size=self.n_batch,
                             shuffle=True,
                             validation_data=(test_data, test_data),
                             callbacks=[checkpoint])


if __name__ == '__main__':
    x_train = x_train.astype('float32') / 255.
    x_test = x_test.astype('float32') / 255.
    x_train = x_train.reshape((len(x_train), np.prod(x_train.shape[1:])))
    x_test = x_test.reshape((len(x_test), np.prod(x_test.shape[1:])))

    test = autoencoder(784, 100, 50, 32, n_epoch=5)
    test.train(x_train, x_test)
    encoded = test.encode(x_test[[0]])
    print(encoded.shape)
    decoded = test.decode(encoded)
    print(np.square(np.subtract(x_test[[0]], decoded)).mean())
