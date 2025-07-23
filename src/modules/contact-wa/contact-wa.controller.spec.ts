import { Test, TestingModule } from '@nestjs/testing';
import { ContactWaController } from './contact-wa.controller';

describe('ContactWaController', () => {
  let controller: ContactWaController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [ContactWaController],
    }).compile();

    controller = module.get<ContactWaController>(ContactWaController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
