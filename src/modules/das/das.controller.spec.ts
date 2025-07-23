import { Test, TestingModule } from '@nestjs/testing';
import { DasController } from './das.controller';

describe('DasController', () => {
  let controller: DasController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [DasController],
    }).compile();

    controller = module.get<DasController>(DasController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
