import { Test, TestingModule } from '@nestjs/testing';
import { MessageWaController } from './message-wa.controller';

describe('MessageWaController', () => {
  let controller: MessageWaController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [MessageWaController],
    }).compile();

    controller = module.get<MessageWaController>(MessageWaController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
